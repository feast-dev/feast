#!/bin/bash

set -euo pipefail

STEP_BREADCRUMB='~~~~~~~~'
SECONDS=0
TIMEFORMAT="${STEP_BREADCRUMB} took %R seconds"

function maybe_build_push_docker {
    # Build and push docker image, tagged with SHA tag, if it doesn't exist already.
    NAME=$1
    TARGET=$NAME-docker
    SUFFIX=feast-$NAME

    if ! aws ecr describe-images --repository-name "feast-ci/feast/$SUFFIX" "--image-ids=imageTag=${GIT_TAG}" >/dev/null ; then
        make "build-$TARGET" "push-$TARGET" REGISTRY="${DOCKER_REPOSITORY}" VERSION="${GIT_TAG}"
    else
        echo "Image ${DOCKER_REPOSITORY}/$SUFFIX:$GIT_TAG already exists, skipping docker build"
    fi
}

source infra/scripts/k8s-common-functions.sh

GIT_TAG=${CODEBUILD_RESOLVED_SOURCE_VERSION}

echo "########## Starting stage $STAGE for ${CODEBUILD_SOURCE_REPO_URL} ${GIT_TAG} ###########"

# This seems to make builds a bit faster.
export DOCKER_BUILDKIT=1

# Workaround for COPY command in core docker image that pulls local maven repo into the image
# itself.
mkdir .m2 2>/dev/null || true
mkdir deps/feast/.m2 2>/dev/null || true

# Log into k8s.
echo "${STEP_BREADCRUMB} Updating kubeconfig"
aws eks update-kubeconfig --name "$EKS_CLUSTER_NAME"

# chmod kubeconfig so it doesn't complain all the time
chmod 755 ~/.kube/config

# Sanity check that kubectl is working.
echo "${STEP_BREADCRUMB} k8s sanity check"
kubectl get pods

case $STAGE in
    core-docker)
        maybe_build_push_docker core
        ;;
    serving-docker)
        maybe_build_push_docker serving
        ;;
    jupyter-docker)
        maybe_build_push_docker jupyter
        ;;
    jobservice-docker)
        maybe_build_push_docker jobservice
        ;;
    ci-docker)
        maybe_build_push_docker ci
        ;;
    e2e-test-emr)
        # EMR test - runs in default namespace.

        # Copy cluster config template generated for us by terraform.
        aws s3 cp "${EMR_TEMPLATE_YML}" emr_cluster.yaml

        # Delete old helm release and PVCs
        k8s_cleanup cicd default

        # Create cluster OR get existing EMR cluster id. In the latter case, clean up any steps
        # already running there from previous test runs.
        echo "${STEP_BREADCRUMB} Creating EMR cluster, this can take up 10 minutes."
        CLUSTER_ID=$(time emr_cluster.py --template emr_cluster.yaml ensure --cleanup)

        # Get (any) node IP. EMR will use this to connect to Kafka and Redis. We make them
        # available to the EMR job by exposing them as NodePort services.
        NODE_IP=$(kubectl get nodes -o custom-columns=Name:.metadata.name | tail -n1)

        # Helm install everything.
        #
        # This may occasionally run into "provided port is already allocated" error due to
        # https://github.com/kubernetes/kubernetes/issues/85894
        helm_install cicd "$DOCKER_REPOSITORY" "$GIT_TAG" default \
            --set "redis.master.service.type=NodePort" \
            --set "redis.master.service.nodePort=32379" \
            --set "kafka.externalAccess.service.type=NodePort" \
            --set "kafka.externalAccess.enabled=true" \
            --set "kafka.externalAccess.service.nodePorts[0]=30092" \
            --set "kafka.externalAccess.service.domain=${NODE_IP}" \
            --set "kafka.service.externalPort=30094"

        # Run the test suite as a one-off pod. We could also run it here, in the codebuild container
        # itself, but that'd require more networking setup to make feast services available
        # outside k8s cluster.
        kubectl delete pod ci-test-runner 2>/dev/null || true

        echo "${STEP_BREADCRUMB} Running the test suite"
        time kubectl run --rm -i ci-test-runner  \
            --restart=Never \
            --image="${DOCKER_REPOSITORY}/feast-ci:${GIT_TAG}" \
            --env="CLUSTER_ID=$CLUSTER_ID" \
            --env="STAGING_PATH=$STAGING_PATH" \
            --env="NODE_IP=$NODE_IP" \
            --  \
            bash -c "mkdir src && cd src && git clone $CODEBUILD_SOURCE_REPO_URL && cd feast* && git config remote.origin.fetch '+refs/pull/*:refs/remotes/origin/pull/*' && git fetch -q && git checkout $CODEBUILD_RESOLVED_SOURCE_VERSION && ./infra/scripts/setup-e2e-env-aws.sh && ./infra/scripts/test-end-to-end-aws.sh"

        ;;
    e2e-test-sparkop)
        # spark k8s test - runs in sparkop namespace (so it doesn't interfere with a concurrently
        # running EMR test).
        NAMESPACE=sparkop
        RELEASE=sparkop

        # Clean up old release
        k8s_cleanup "$RELEASE" "$NAMESPACE"

        # Helm install everything in a namespace
        helm_install "$RELEASE" "${DOCKER_REPOSITORY}" "${GIT_TAG}" "$NAMESPACE"

        # Delete old test running pod if it exists
        kubectl delete pod -n "$NAMESPACE" ci-test-runner 2>/dev/null || true

        # Delete all sparkapplication resources that may be left over from the previous test runs.
        kubectl delete sparkapplication --all -n "$NAMESPACE" || true

        # Make sure the test pod has permissions to create sparkapplication resources
        setup_sparkop_role

        # Run the test suite as a one-off pod.
        echo "${STEP_BREADCRUMB} Running the test suite"
        if ! time kubectl run --rm -n "$NAMESPACE" -i ci-test-runner  \
            --restart=Never \
            --image="${DOCKER_REPOSITORY}/feast-ci:${GIT_TAG}" \
            --env="STAGING_PATH=$STAGING_PATH" \
            --  \
            bash -c "mkdir src && cd src && git clone $CODEBUILD_SOURCE_REPO_URL && cd feast* && git config remote.origin.fetch '+refs/pull/*:refs/remotes/origin/pull/*' && git fetch -q && git checkout $CODEBUILD_RESOLVED_SOURCE_VERSION && ./infra/scripts/setup-e2e-env-sparkop.sh && ./infra/scripts/test-end-to-end-sparkop.sh" ; then

            readarray -t CRASHED_PODS < <(kubectl get pods --no-headers=true --namespace sparkop | grep Error | awk '{ print $1 }')

            for POD in "${CRASHED_PODS[@]}"; do
                echo "Logs from crashed pod $POD:"
                kubectl logs --namespace sparkop "$POD"
            done
        fi

        ;;
    cleanup)
        emr_cluster.py --template emr_cluster.yaml destroy
        ;;
    *)
        echo "Unknown stage $STAGE"
        ;;
esac

echo "########## Stage $STAGE took $SECONDS seconds ###########"
