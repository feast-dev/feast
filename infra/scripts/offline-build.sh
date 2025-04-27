# to run -> source ./infra/scripts/offline-build.sh
# on the build host... requires docker/podman, git, wget

APACHE_ARROW_VERSION="17.0.0"
SUBSTRAIT_VERSION="0.44.0"
IBIS_VERSION="9.5.0"

# Get Feast project repository root directory
PROJECT_ROOT_DIR=$(git rev-parse --show-toplevel)
OFFLINE_BUILD_DIR=${PROJECT_ROOT_DIR}/offline_build
cd ${PROJECT_ROOT_DIR}

rm -rf ./offline_build
mkdir offline_build

# yum builder
docker build \
  --tag yum-builder \
  -f sdk/python/feast/infra/feature_servers/multicloud/offline/Dockerfile.builder.yum \
  sdk/python/feast/infra/feature_servers/multicloud/offline

git clone --branch apache-arrow-${APACHE_ARROW_VERSION} https://github.com/apache/arrow ${OFFLINE_BUILD_DIR}/arrow
${OFFLINE_BUILD_DIR}/arrow/cpp/thirdparty/download_dependencies.sh ${OFFLINE_BUILD_DIR}/arrow/cpp/arrow-thirdparty
wget https://github.com/substrait-io/substrait/archive/v${SUBSTRAIT_VERSION}.tar.gz -O ${OFFLINE_BUILD_DIR}/arrow/cpp/arrow-thirdparty/substrait-${SUBSTRAIT_VERSION}.tar.gz

alias hermeto='docker run --rm -ti -v "$PWD:$PWD:Z" -w "$PWD" quay.io/konflux-ci/hermeto:0.24.0'
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

# arrow OFFLINE builder
rm -f ${OFFLINE_BUILD_DIR}/arrow/.dockerignore
docker build \
  --network none \
  --volume ${OFFLINE_BUILD_DIR}/arrow:/tmp/arrow:Z \
  --volume ${OFFLINE_BUILD_DIR}/hermeto-output:/tmp/hermeto-output:Z \
  --volume ${OFFLINE_BUILD_DIR}/hermeto.env:/tmp/hermeto.env:Z \
  --tag arrow-builder \
  -f sdk/python/feast/infra/feature_servers/multicloud/offline/Dockerfile.builder.arrow \
  ${OFFLINE_BUILD_DIR}/arrow

# ibis OFFLINE builder - ibis-framework must build from its own git repo... versioning requirement
git clone -b ${IBIS_VERSION} https://github.com/ibis-project/ibis ${OFFLINE_BUILD_DIR}/ibis
docker build \
  --network none \
  --volume ${OFFLINE_BUILD_DIR}/hermeto-output:/tmp/hermeto-output:Z \
  --volume ${OFFLINE_BUILD_DIR}/hermeto.env:/tmp/hermeto.env:Z \
  --tag ibis-builder \
  -f sdk/python/feast/infra/feature_servers/multicloud/offline/Dockerfile.builder.ibis \
  ${OFFLINE_BUILD_DIR}

# feast OFFLINE builder
docker build \
  --network none \
  --volume ${OFFLINE_BUILD_DIR}/hermeto-yarn-ui-output:/tmp/hermeto-yarn-ui-output:Z \
  --volume ${OFFLINE_BUILD_DIR}/hermeto-yarn-ui.env:/tmp/hermeto-yarn-ui.env:Z \
  --volume ${OFFLINE_BUILD_DIR}/hermeto-yarn-output:/tmp/hermeto-yarn-output:Z \
  --volume ${OFFLINE_BUILD_DIR}/hermeto-yarn.env:/tmp/hermeto-yarn.env:Z \
  --volume ${OFFLINE_BUILD_DIR}/hermeto-output:/tmp/hermeto-output:Z \
  --volume ${OFFLINE_BUILD_DIR}/hermeto.env:/tmp/hermeto.env:Z \
  --tag feature-server:sdist-build \
  -f sdk/python/feast/infra/feature_servers/multicloud/offline/Dockerfile.sdist \
  ${PROJECT_ROOT_DIR}

docker run --rm -ti feature-server:sdist-build feast version
