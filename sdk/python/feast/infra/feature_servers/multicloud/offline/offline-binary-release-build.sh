# to run -> source ./sdk/python/feast/infra/feature_servers/multicloud/offline/offline-binary-release-build.sh
# on the build host... requires docker/podman

# Get Feast project repository root directory
PROJECT_ROOT_DIR=$(git rev-parse --show-toplevel)
OFFLINE_BUILD_DIR=${PROJECT_ROOT_DIR}/offline_build
cd ${PROJECT_ROOT_DIR}

rm -rf ./offline_build
mkdir offline_build

alias hermeto='docker run --rm -ti -v "$PWD:$PWD:Z" -w "$PWD" quay.io/konflux-ci/hermeto:0.25.0'
hermeto fetch-deps \
  --output ${OFFLINE_BUILD_DIR}/hermeto-output \
  '{
  "type": "pip",
  "path": ".",
  "requirements_files": [
"sdk/python/feast/infra/feature_servers/multicloud/requirements.txt"
],
  "requirements_build_files": [
"sdk/python/requirements/py3.11-minimal-requirements.txt",
"sdk/python/requirements/py3.11-minimal-sdist-requirements.txt",
"sdk/python/requirements/py3.11-minimal-sdist-requirements-build.txt"
],
  "allow_binary": "true"
}'
hermeto generate-env -o ${OFFLINE_BUILD_DIR}/hermeto.env --for-output-dir /tmp/hermeto-output ${OFFLINE_BUILD_DIR}/hermeto-output

# feast OFFLINE builder
docker build \
  --volume ${OFFLINE_BUILD_DIR}/hermeto-output:/tmp/hermeto-output:Z \
  --volume ${OFFLINE_BUILD_DIR}/hermeto.env:/tmp/hermeto.env:Z \
  --network none \
  --tag feature-server:binary-release-build \
  -f sdk/python/feast/infra/feature_servers/multicloud/offline/Dockerfile.binary.release \
  sdk/python/feast/infra/feature_servers/multicloud

docker run --rm -ti feature-server:binary-release-build feast version
