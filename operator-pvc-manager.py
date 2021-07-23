#!/usr/bin/env python3

"""
Manage PersistentVolumeClaims beyond what is provided by Kubernetes.

operator-pvc-manager expects two K8s annotations to help guide it:
  * operator-pvc-manager/storage-size on the STS with the VCT. If the PVC is smaller
    than storage-size, then it is sized up.
  * operator-pvc-manager/statefulset on the PVC that was created by the STS. If the STS
    is missing or has more PVC than needed, the PVC is deleted.

The operator is expected to run as a CronJob in Kubernetes, and needs a
ClusterRole with sufficient permissions to list STS, and list and patch PVCs.

Log level defaults to INFO but will be lowered to DEBUG if ${OPERATOR_DEBUG}
environment variable is set.
"""

import logging
import os
from kubernetes import client, config


def main():
    """Loop through all PVCs and perform necessary actions."""
    pvcs = v1.list_persistent_volume_claim_for_all_namespaces().items

    for pvc in pvcs:
        logging.debug(f"Processing PVC {pvc.metadata.namespace}.{pvc.metadata.name}")
        if not pvc.metadata.annotations.get('operator-pvc-manager/statefulset'): continue
        if delete_if_needed(pvc): continue
        resize_if_needed(pvc)

def delete_if_needed(pvc):
    """Delete the PVC if the associated STS is scaled down or deleted.

    Return True if deleted, False otherwise.
    """
    logging.debug(f"delete_if_needed {pvc.metadata.namespace}.{pvc.metadata.name}")
    # Find the associated statefulset
    sts = get_sts_for_pvc(pvc)

    # Delete if STS is missing
    if not sts:
        logging.info(f"Deleting orphaned PVC {pvc.metadata.namespace}.{pvc.metadata.name}")
        v1.delete_namespaced_persistent_volume_claim(
            name      = pvc.metadata.name,
            namespace = pvc.metadata.namespace
        )
        return True
    
    # Delete if STS is scaled down
    sts_replicas = sts.spec.replicas
    pvc_ordinal = get_ordinal(pvc)
    if pvc_ordinal >= sts_replicas:  # the highest PVC should be replicas - 1
        logging.info(f"Deleting downscaled PVC {pvc.metadata.namespace}.{pvc.metadata.name}")
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
    logging.debug(f"resize_if_needed {pvc.metadata.namespace}.{pvc.metadata.name}")

    sts = get_sts_for_pvc(pvc)
    desired_size = get_pvc_desired_size(sts)

    if pvc.spec.resources.requests['storage'] == desired_size:
        logging.debug(f"Size already matches")
        return False
    if pvc.spec.resources.requests['storage'] > desired_size:
        logging.warning(f"PVC {pvc.metadata.namespace}.{pvc.metadata.name} is larger than desired size {desired_size}")
        return False

    # Patch the PVC with the new size
    logging.info(f"Resizing PVC {pvc.metadata.namespace}.{pvc.metadata.name} to {desired_size}")
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
    
    This is done by following the operator-pvc-manager/statefulset annotation. If the
    annotation itself is missing, throw a RuntimeWarning.
    
    If multiple STS are found, throw a RuntimeError instead."""
    # Grab the pointer
    sts_pointer = pvc.metadata.annotations.get('operator-pvc-manager/statefulset')

    # A missing pointer means this isn't a managed PVC. It shouldn't have
    # been passed to this function in the first place, because main() already
    # checks for the annotation. This is probably a mistake.
    if not sts_pointer:
        logging.warning(f"get_sts_for_pvc was passed a non-managed PVC {pvc.metadata.namespace}.{pvc.metadata.name}")
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
        logging.error(f"get_sts_for_pvc found multiple STS matching pointer {sts_pointer}. This shouldn't be possible. Bailing!")
        logging.error(sts_list)
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
        logging.error("get_ordinal received a non-ordered input")
        raise RuntimeWarning("get_ordinal received a non-ordered input")
    return int(ordinal)

def get_pvc_desired_size(sts):
    """Return the desired size of the PersistentVolumes for this StatefulSet.

    Throws a RuntimeWarning if the operator-pvc-manager/storage-size annotation is
    missing or incorrectly formatted.
    """
    desired_size = sts.metadata.annotations.get('operator-pvc-manager/storage-size')
    if not desired_size:
        logging.warning("get_pvc_desired_size received STS {sts.metadata.namespace}.{sts.metadata.name} with missing annotation")
        raise RuntimeWarning("get_pvc_desired_size received STS with missing annotation")
    if (not desired_size.endswith("Gi") or
        not desired_size.split("Gi")[0].isnumeric()):
        logging.warning("get_pvc_desired_size received STS {sts.metadata.namespace}.{sts.metadata.name} with malformed annotation")
        raise RuntimeWarning("get_pvc_desired_size received STS with malformed annotation")
    return desired_size

if __name__== "__main__":
    # Configure logging
    if os.environ.get('OPERATOR_DEBUG'):
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=loglevel)
    logging.getLogger('kubernetes').setLevel(logging.WARNING)  # otherwise it inherits root loglevel, very spammy

    logging.info("Started PVC Operator")

    # Find the kubeconfig dynamically, allows for local testing
    if os.path.exists("/var/run/secrets/kubernetes.io/serviceaccount/namespace"):
        logging.info("Working in kubernetes environment")
        config.load_incluster_config()
    else:
        logging.info("Working in local environment talking to a remote kubernetes cluster")
        config.load_kube_config()

    # Start a kubernetes Core V1 client
    v1 = client.CoreV1Api()
    # Start an Apps V1 Client (statefulsets)
    appsv1 = client.AppsV1Api()

    logging.info("Loaded up Kubernetes Clients... let's continue")
    main()
    logging.info("Bye.")
