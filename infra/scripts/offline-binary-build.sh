# to run -> source ./infra/scripts/offline-binary-build.sh
# on the build host... requires docker/podman

# Get Feast project repository root directory
PROJECT_ROOT_DIR=$(git rev-parse --show-toplevel)
OFFLINE_BUILD_DIR=${PROJECT_ROOT_DIR}/offline_build
cd ${PROJECT_ROOT_DIR}

rm -rf ./offline_build
mkdir offline_build

alias cachi2='docker run --rm -ti -v "$PWD:$PWD:z" -w "$PWD" quay.io/konflux-ci/cachi2:f7a61b067f4446e4982d0e3b9545ce4aa0d8284f'
cachi2 fetch-deps \
  --output ${OFFLINE_BUILD_DIR}/cachi2-output \
  '{
  "type": "pip",
  "path": ".",
  "requirements_files": [
"sdk/python/feast/infra/feature_servers/multicloud/requirements.txt"
],
  "requirements_build_files": [
"sdk/python/requirements/py3.11-minimal-requirements.txt",
"sdk/python/requirements/py3.11-sdist-requirements.txt"
],
  "allow_binary": "true"
}'

cachi2 generate-env ${OFFLINE_BUILD_DIR}/cachi2-output -o ${OFFLINE_BUILD_DIR}/cachi2.env --for-output-dir /tmp/cachi2-output

# feast OFFLINE builder
docker build \
  --volume ${OFFLINE_BUILD_DIR}/cachi2-output:/tmp/cachi2-output:Z \
  --volume ${OFFLINE_BUILD_DIR}/cachi2.env:/tmp/cachi2.env:Z \
  --network none \
  --tag feature-server:binary-build \
  -f sdk/python/feast/infra/feature_servers/multicloud/offline/Dockerfile.binary \
  --load sdk/python/feast/infra/feature_servers/multicloud

docker run --rm -ti feature-server:binary-build feast version
