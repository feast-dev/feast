# to run -> source ./sdk/python/feast/infra/feature_servers/multicloud/offline/offline-release-build.sh
# on the build host... requires docker/podman, git, wget

# Get Feast project repository root directory
PROJECT_ROOT_DIR=$(git rev-parse --show-toplevel)
OFFLINE_BUILD_DIR=${PROJECT_ROOT_DIR}/offline_build
cd ${PROJECT_ROOT_DIR}

rm -rf ./offline_build
mkdir offline_build

alias hermeto='docker run --rm -ti -v "$PWD:$PWD:Z" -w "$PWD" quay.io/konflux-ci/hermeto:0.25.0'
hermeto fetch-deps \
  --source sdk/python/feast/infra/feature_servers/multicloud/offline \
  --output ${OFFLINE_BUILD_DIR}/hermeto-rpm-output \
  --dev-package-managers \
  rpm

hermeto fetch-deps \
  --source sdk/python/feast/infra/feature_servers/multicloud/offline \
  --output ${OFFLINE_BUILD_DIR}/hermeto-generic-output \
  generic

hermeto fetch-deps \
  --output ${OFFLINE_BUILD_DIR}/hermeto-output \
  '{
  "type": "pip",
  "path": ".",
  "requirements_files": [
"sdk/python/feast/infra/feature_servers/multicloud/requirements.txt"
],
  "requirements_build_files": [
"sdk/python/requirements/py3.11-minimal-sdist-requirements.txt",
"sdk/python/requirements/py3.11-minimal-sdist-requirements-build.txt"
],
  "allow_binary": "false"
}'
hermeto generate-env -o ${OFFLINE_BUILD_DIR}/hermeto.env --for-output-dir /tmp/hermeto-output ${OFFLINE_BUILD_DIR}/hermeto-output
hermeto inject-files --for-output-dir /tmp/hermeto-output ${OFFLINE_BUILD_DIR}/hermeto-output

# feast OFFLINE builder
docker build \
  --network none \
  --volume ${OFFLINE_BUILD_DIR}/hermeto-output:/tmp/hermeto-output:Z \
  --volume ${OFFLINE_BUILD_DIR}/hermeto.env:/tmp/hermeto.env:Z \
  --volume ${OFFLINE_BUILD_DIR}/hermeto-generic-output:/tmp/hermeto-generic-output:Z \
  --volume ${OFFLINE_BUILD_DIR}/hermeto-rpm-output:/tmp/hermeto-rpm-output:Z \
  --tag feature-server:sdist-release-build \
  -f sdk/python/feast/infra/feature_servers/multicloud/offline/Dockerfile.sdist.release \
  sdk/python/feast/infra/feature_servers/multicloud

docker run --rm -ti feature-server:sdist-release-build feast version
