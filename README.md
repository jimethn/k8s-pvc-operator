# operator-pvc-manager

Responsibilities:

1. Resize PVCs
2. Delete orphaned PVCs
3. (TODO) Delete & replace STS so that scale up doesn't get the wrong size

Requirements:

- AWS cloud

## How to use:

If you want this tool to manage PVC in your kube cluster, it requires two annotations be added to your manifests:

1. The StatefulSet must have the annotation `pvc-operator/storage-size: <Size>` (e.g. 500Gi)
2. The STS's VolumeClaimTemplate must have the annotation `pvc-operator/statefulset: <StatefulSet name>`

You should set the VolumeClaimTemplate to a static "initial" size. This is to avoid an immutability error when trying to modify the STS. When the operator runs, it will scale up any under-sized PVCs.

## How to configure:

The tool can be configured via environment variables.

* **OPERATOR_DEBUG**: If this variable is set (to any value), debug-level logging will be used in the service.
* **PVC_GRACE_MINUTES**: How long a PVC must be orphaned before being considered for deletion. Defaults to 60.

