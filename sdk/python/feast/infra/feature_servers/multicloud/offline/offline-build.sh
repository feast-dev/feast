# to run -> source ./sdk/python/feast/infra/feature_servers/multicloud/offline/offline-build.sh
# on the build host... requires docker/podman, git, wget

# Get Feast project repository root directory
PROJECT_ROOT_DIR=$(git rev-parse --show-toplevel)
OFFLINE_BUILD_DIR=${PROJECT_ROOT_DIR}/offline_build
cd ${PROJECT_ROOT_DIR}

rm -rf ./offline_build
mkdir offline_build

# yarn builder
docker build \
  --tag yarn-builder \
  -f sdk/python/feast/infra/feature_servers/multicloud/offline/Dockerfile.builder.yarn \
  sdk/python/feast/infra/feature_servers/multicloud/offline

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

# not needed for downstream build from release
###############################
hermeto fetch-deps \
  --output ${OFFLINE_BUILD_DIR}/hermeto-yarn-ui-output \
  '{
  "type": "yarn",
  "path": "ui"
}'
hermeto generate-env -o ${OFFLINE_BUILD_DIR}/hermeto-yarn-ui.env --for-output-dir /tmp/hermeto-yarn-ui-output ${OFFLINE_BUILD_DIR}/hermeto-yarn-ui-output

hermeto fetch-deps \
  --output ${OFFLINE_BUILD_DIR}/hermeto-yarn-output \
  '{
  "type": "yarn",
  "path": "sdk/python/feast/ui"
}'
hermeto generate-env -o ${OFFLINE_BUILD_DIR}/hermeto-yarn.env --for-output-dir /tmp/hermeto-yarn-output ${OFFLINE_BUILD_DIR}/hermeto-yarn-output
###############################

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
  --volume ${OFFLINE_BUILD_DIR}/hermeto-yarn-ui-output:/tmp/hermeto-yarn-ui-output:Z \
  --volume ${OFFLINE_BUILD_DIR}/hermeto-yarn-ui.env:/tmp/hermeto-yarn-ui.env:Z \
  --volume ${OFFLINE_BUILD_DIR}/hermeto-yarn-output:/tmp/hermeto-yarn-output:Z \
  --volume ${OFFLINE_BUILD_DIR}/hermeto-yarn.env:/tmp/hermeto-yarn.env:Z \
  --volume ${OFFLINE_BUILD_DIR}/hermeto-output:/tmp/hermeto-output:Z \
  --volume ${OFFLINE_BUILD_DIR}/hermeto.env:/tmp/hermeto.env:Z \
  --volume ${OFFLINE_BUILD_DIR}/hermeto-generic-output:/tmp/hermeto-generic-output:Z \
  --volume ${OFFLINE_BUILD_DIR}/hermeto-rpm-output:/tmp/hermeto-rpm-output:Z \
  --tag feature-server:sdist-build \
  -f sdk/python/feast/infra/feature_servers/multicloud/offline/Dockerfile.sdist \
  ${PROJECT_ROOT_DIR}

docker run --rm -ti feature-server:sdist-build feast version
