#!/usr/bin/env python3

"""
Manage PersistentVolumeClaims beyond what is provided by Kubernetes.

operator-pvc-manager expects two K8s annotations to help guide it:
  * pvc-operator/storage-size on the STS with the VCT. If the PVC is smaller
    than storage-size, then it is sized up.
  * pvc-operator/statefulset on the PVC that was created by the STS. If the STS
    is missing or has more PVC than needed, the PVC is deleted.

The operator is expected to run as a single-replica Deployment in Kubernetes,
and needs a ClusterRole with sufficient permissions to list STS, and list and
patch PVCs. It needs cloudtrail:LookupEvents IAM to check the PVC status.

Log level defaults to INFO but will be lowered to DEBUG if ${OPERATOR_DEBUG}
environment variable is set.
"""

import boto3
import logging
import os
from datetime import datetime, timedelta, timezone
from kubernetes import client, config
from time import sleep

def main():
    """Loop through all PVCs and perform necessary actions."""
    while True:
        logger.debug("Starting main() loop")

        pvcs = v1.list_persistent_volume_claim_for_all_namespaces().items
        for pvc in pvcs:
            logger.debug(f"Processing PVC {pvc.metadata.namespace}.{pvc.metadata.name}")
            if not pvc.metadata.annotations.get('pvc-operator/statefulset'): continue
            if delete_if_needed(pvc): continue
            resize_if_needed(pvc)

        health_check()
        sleep(30)

def delete_if_needed(pvc):
    """Delete the PVC if the associated STS is scaled down or deleted.

    Return True if deleted, False otherwise.
    """
    logger.debug(f"delete_if_needed {pvc.metadata.namespace}.{pvc.metadata.name}")
    # Find the associated statefulset
    sts = get_sts_for_pvc(pvc)

    # Delete if STS is missing
    if not sts:
        if not pvc_unmounted_long_enough(pvc):
            return False
        logger.info(f"Deleting orphaned PVC {pvc.metadata.namespace}.{pvc.metadata.name}")
        v1.delete_namespaced_persistent_volume_claim(
            name      = pvc.metadata.name,
            namespace = pvc.metadata.namespace
        )
        return True
    
    # Delete if STS is scaled down
    sts_replicas = sts.spec.replicas
    pvc_ordinal = get_ordinal(pvc)
    if (pvc_ordinal >= sts_replicas  # the highest PVC should be replicas - 1
        and pvc_unmounted_long_enough(pvc)):
        logger.info(f"Deleting downscaled PVC {pvc.metadata.namespace}.{pvc.metadata.name}")
        v1.delete_namespaced_persistent_volume_claim(
            name      = pvc.metadata.name,
            namespace = pvc.metadata.namespace
        )
        return True
    
    # Nothing was deleted
    return False

def resize_if_needed(pvc):
    """Grow the PVC if the desired size has increased.

    Shrinking PVC is not supported, because shrinking EBS is not supported by AWS.

    Return True if resized, False otherwise.
    """
    logger.debug(f"resize_if_needed {pvc.metadata.namespace}.{pvc.metadata.name}")

    sts = get_sts_for_pvc(pvc)
    current_size = pvc.spec.resources.requests['storage'].strip()
    desired_size = get_pvc_desired_size(sts).strip()

    # Validate the inputs. Units other than Gi should not have made it past
    # linting, but that doesn't relieve us of our duty to check.
    if current_size[-2:] != 'Gi':
        logger.warning(f"PVC {pvc.metadata.namespace}.{pvc.metadata.name} has invalid units. Doing nothing.")
        return False
    if desired_size[-2:] != 'Gi':
        logger.warning(f"STS {sts.metadata.namespace}.{sts.metadata.name} has invalid units. Doing nothing.")
        return False

    # Strip the unit off the end so we can do numeric comparison
    current_size_int = int(current_size[:-2])
    desired_size_int = int(desired_size[:-2])

    if current_size_int == desired_size_int:
        logger.debug(f"...size already matches")
        return False
    if current_size_int > desired_size_int:
        logger.warning(f"PVC {pvc.metadata.namespace}.{pvc.metadata.name} is larger than desired size {desired_size}")
        return False

    # Patch the PVC with the new size
    logger.info(f"Resizing PVC {pvc.metadata.namespace}.{pvc.metadata.name} to {desired_size}")
    patch = {"spec": {"resources": {"requests": {"storage": desired_size}}}}
    v1.patch_namespaced_persistent_volume_claim(
        namespace = pvc.metadata.namespace,
        name      = pvc.metadata.name,
        body      = patch
    )
    return True

def get_sts_for_pvc(pvc):
    """Return the single StatefulSet object for this PersistentVolumeClaim,
    or False if the StatefulSet doesn't exist.
    
    This is done by following the pvc-operator/statefulset annotation. If the
    annotation itself is missing, throw a RuntimeWarning.
    
    If multiple STS are found, throw a RuntimeError instead."""
    # Grab the pointer
    sts_pointer = pvc.metadata.annotations.get('pvc-operator/statefulset')

    # A missing pointer means this isn't a managed PVC. It shouldn't have
    # been passed to this function in the first place, because main() already
    # checks for the annotation. This is probably a mistake.
    if not sts_pointer:
        logger.warning(f"get_sts_for_pvc was passed a non-managed PVC {pvc.metadata.namespace}.{pvc.metadata.name}")
        # We raise an exception instead of returning False, because there's a
        # difference between the pointer referencing a missing object, and the
        # pointer itself being missing. Mistaking the two could mean deleting
        # something we're not supposed to.
        raise RuntimeWarning("get_sts_for_pvc was passed a non-managed PVC")

    # Follow the pointer
    sts_list = appsv1.list_namespaced_stateful_set(
        namespace      = pvc.metadata.namespace,
        field_selector = f'metadata.name={sts_pointer}'
    )

    # Die if more than one STS is found. How is this even possible given a
    # filter on metadata.name? Probably this would never come up, but let's
    # check anyway.
    if len(sts_list.items) > 1:
        logger.error(f"get_sts_for_pvc found multiple STS matching pointer {sts_pointer}. This shouldn't be possible. Bailing!")
        logger.error(sts_list)
        raise RuntimeError("get_sts_for_pvc found multiple matching STS")
    
    # An empty list means the STS has been deleted and the PVC is orphaned.
    if not sts_list.items:
        return False

    # We've found the associated StatefulSet. Return it.
    return sts_list.items[0]

def get_ordinal(obj):
    """Return the ordinal of the Kubernetes Object.
    
    Throws a RuntimeWarning if the Object doesn't end with a number."""
    # Each PersistentVolumeClaim created by a VolumeClaimTemplate is named like
    #    VolumeName-PodName-Ordinal
    # where ordinal matches the associated Pod.
    # In the same way, each Pod created by a StatefulSet is named like
    #    StatefulSetName-Ordinal
    # Generically, that means we can return the ordinal of any ordered object by
    # splitting on hyphens and returning the last item.
    # For more information, see: https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/#ordinal-index
    ordinal = obj.metadata.name.split('-')[-1]
    if not ordinal.isnumeric():
        logger.error("get_ordinal received a non-ordered input")
        raise RuntimeWarning("get_ordinal received a non-ordered input")
    return int(ordinal)

def get_pvc_desired_size(sts):
    """Return the desired size of the PersistentVolumes for this StatefulSet.

    Throws a RuntimeWarning if the pvc-operator/storage-size annotation is
    missing or incorrectly formatted.
    """
    desired_size = sts.metadata.annotations.get('pvc-operator/storage-size')
    if not desired_size:
        logger.warning(f"get_pvc_desired_size received STS {sts.metadata.namespace}.{sts.metadata.name} with missing annotation")
        raise RuntimeWarning("get_pvc_desired_size received STS with missing annotation")
    if (not desired_size.endswith("Gi") or
        not desired_size.split("Gi")[0].isnumeric()):
        logger.warning(f"get_pvc_desired_size received STS {sts.metadata.namespace}.{sts.metadata.name} with malformed annotation")
        raise RuntimeWarning("get_pvc_desired_size received STS with malformed annotation")
    return desired_size

def pvc_unmounted_long_enough(pvc):
    """Return True if the volume has been unmounted long enough to consider
    deleting it. Configured via PVC_GRACE_MINUTES env var, defaults to 1h.

    Finds the volume in AWS to check the unmount time. Kubernetes does not
    expose this information.
    """
    logger.debug(f"pvc_unmounted_long_enough {pvc.metadata.namespace}.{pvc.metadata.name}")
    # Find the EBS volume ID from kube
    pv_name = pvc.spec.volume_name
    pv = v1.list_persistent_volume(field_selector=f'metadata.name={pv_name}')
    ebs_volume = pv.items[0].spec.aws_elastic_block_store.volume_id.split('/')[-1]  # formatted like 'aws://us-east-1c/vol-0a6d7a39a07212c42'

    # Look through CloudTrail events for that volume. This API is rate limited to 2 TPS.
    events = cloudtrail.lookup_events(
        LookupAttributes=[
            {
                'AttributeKey': 'ResourceName',
                'AttributeValue': ebs_volume
            },
        ],
        MaxResults=30  # the event we're looking for could get drowned out by other stuff like tagging
    )
    for event in events['Events']:
        # The event list is pre-sorted with most recent first, so we just find
        # the first attachment-related event and return.
        if event['EventName'] == 'AttachVolume':
            # It's been attached most recently, it is not safe to delete
            logger.debug("...it still looks attached, return false")
            return False
        if event['EventName'] == 'DetachVolume':
            # It's been detatched most recently, but how long ago?
            time_since_detatch = datetime.now(timezone.utc) - event['EventTime']
            if time_since_detatch > pvc_grace_minutes:
                # It's been detatched long enough, OK to delete
                logger.debug("...it's been detatched a while, return true")
                return True
            else:
                logger.debug("...it hasn't been detatched long, return false")
                return False

    # If we got here, we didn't find any Attach/Detach events. CloudTrail has
    # a ~5 minute delay, but unless we are rapidly scaling a new STS up and
    # down I'd still expect to see something. Maybe the interesting events
    # got drowned out by other stuff like tagging? We grabbed 30 results so
    # that's a _lot_ of tagging...
    logger.warning(f"Didn't find any attach or detatch events for PVC {pvc.metadata.namespace}.{pvc.metadata.name}")
    return False

def ready_check(check_file='/tmp/heartbeat'):
    """Verifies dependencies and writes out /tmp/heartbeat."""
    # Verify network and permissions by making each of the major API calls
    v1.list_persistent_volume_claim_for_all_namespaces()
    appsv1.list_stateful_set_for_all_namespaces()
    cloudtrail.lookup_events(MaxResults=1)
    # If we got this far without exception, we are ready
    with open(check_file, 'w') as f:
        f.write('ready\n')

def health_check(check_file='/tmp/heartbeat'):
    """Writes out /tmp/heartbeat."""
    with open(check_file, 'w') as f:
        f.write('running\n')

if __name__ == "__main__":
    # Log everything to both stdout and to file
    logFormatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
    rootLogger = logging.getLogger()
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    rootLogger.addHandler(consoleHandler)
    fileHandler = logging.FileHandler("/app/logs/operator-pvc-manager.log")
    fileHandler.setFormatter(logFormatter)
    rootLogger.addHandler(fileHandler)
    # Create our own logger for this app and set log level
    logger = logging.getLogger(__name__)
    if os.environ.get('OPERATOR_DEBUG'):
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    # Configure how long a PVC must be detatched before we will delete it
    if os.environ.get('PVC_GRACE_MINUTES'):
        pvc_grace_minutes = timedelta(minutes=int(os.environ.get('PVC_GRACE_MINUTES')))
    else:
        pvc_grace_minutes = timedelta(hours=1)

    logger.info("Started PVC Operator")

    # Find the kubeconfig dynamically, allows for local testing
    if os.path.exists("/var/run/secrets/kubernetes.io/serviceaccount/namespace"):
        logger.info("Working in kubernetes environment")
        config.load_incluster_config()
    else:
        logger.info("Working in local environment talking to a remote kubernetes cluster")
        config.load_kube_config()

    # Start a kubernetes Core V1 client
    v1 = client.CoreV1Api()
    # Start an Apps V1 Client (statefulsets)
    appsv1 = client.AppsV1Api()
    # Start an AWS CloudTrail Client
    cloudtrail = boto3.client('cloudtrail')  # TODO do we need to auto-refresh this token?

    # Verify networking and permissions
    ready_check()

    logger.info("Loaded up Kubernetes and AWS clients... let's continue")
    main()
    logger.info("Bye.")
