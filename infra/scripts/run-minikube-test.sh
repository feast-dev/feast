#!/bin/bash

set -euo pipefail

NAMESPACE=sparkop
JOB_NAME=test-runner

# Delete all sparkapplication resources that may be left over from the previous test runs.
kubectl delete sparkapplication --all -n sparkop || true

JOB_SPEC=$(dirname $0)/test_job.yaml

# Delete previous instance of the job if it exists
kubectl delete -n ${NAMESPACE} "job/$JOB_NAME"  2>/dev/null || true

# Create the job
kubectl apply -n ${NAMESPACE} -f "$JOB_SPEC"

# Wait for job to have a pod.
for i in {1..10}
do
	POD=$(kubectl get pods -n ${NAMESPACE} --selector=job-name=$JOB_NAME --output=jsonpath='{.items[0].metadata.name}')
	if [ ! -z "$POD" ]; then
		break
	else
		sleep 1
	fi
done

echo "Waiting for pod to be ready:"
kubectl wait -n ${NAMESPACE} --for=condition=ContainersReady "pod/$POD" --timeout=60s || true

echo "Job output:"
kubectl logs -n ${NAMESPACE} -f "job/$JOB_NAME"

# Can't wait for both conditions at once, so wait for complete first then wait for failure
kubectl wait -n ${NAMESPACE} --for=condition=complete "job/$JOB_NAME" --timeout=60s && exit 0
kubectl wait -n ${NAMESPACE} --for=condition=failure "job/$JOB_NAME" --timeout=60s && exit 1
