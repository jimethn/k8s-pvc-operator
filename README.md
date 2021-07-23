# operator-pvc-manager

Responsibilities:

1. Resize PVCs
2. Delete orphaned PVCs
3. (TODO) Delete & replace STS so that scale up doesn't get the wrong size

## How to setup:

The operator is expected to run as a CronJob in Kubernetes, and needs a ClusterRole with sufficient permissions to list STS, and list and patch PVCs.

## How to use:

If you want this tool to manage PVC in your kube cluster, it requires two annotations be added to your helm charts.

1. The StatefulSet must have the annotation `operator-pvc-manager/storage-size: 500Gi` (replace 500 with the correct size)
2. The STS's VolumeClaimTemplate must have the annotation `operator-pvc-manager/statefulset: <StatefulSet name>`

Additionally, you should set the VolumeClaimTemplate to a static "initial" size. This is to avoid an immutability error when trying to modify the STS. When the operator runs, it will scale up any under-sized PVCs.
