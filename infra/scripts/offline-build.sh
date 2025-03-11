# to run -> source ./infra/scripts/offline-build.sh
# on the build host... requires docker/podman, git #### uv, conan==1.64.1, cmake, libstdc++, gcc, g++

# Get Feast project repository root directory
PROJECT_ROOT_DIR=$(git rev-parse --show-toplevel)
OFFLINE_BUILD_DIR=${PROJECT_ROOT_DIR}/offline_build
cd ${PROJECT_ROOT_DIR}

rm -rf ./offline_build
mkdir offline_build

# yum builder
docker build \
  --tag yum-builder \
  -f sdk/python/feast/infra/feature_servers/multicloud/Dockerfile.builder.yum \
  --load sdk/python/feast/infra/feature_servers/multicloud

git clone --branch apache-arrow-17.0.0 https://github.com/apache/arrow ${OFFLINE_BUILD_DIR}/arrow
${OFFLINE_BUILD_DIR}/arrow/cpp/thirdparty/download_dependencies.sh ${OFFLINE_BUILD_DIR}/arrow/cpp/arrow-thirdparty

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
"sdk/python/requirements/py3.11-sdist-requirements.txt",
"sdk/python/requirements/py3.11-pandas-requirements.txt",
"sdk/python/requirements/py3.11-postgres-source-requirements.txt",
"sdk/python/requirements/py3.11-addtl-sources-requirements.txt"
],
  "allow_binary": "false"
}'
cachi2 generate-env ${OFFLINE_BUILD_DIR}/cachi2-output -o ${OFFLINE_BUILD_DIR}/cachi2.env --for-output-dir /tmp/cachi2-output

# arrow OFFLINE builder - version 17.0.0
rm ${OFFLINE_BUILD_DIR}/arrow/.dockerignore
docker build \
  --volume ${OFFLINE_BUILD_DIR}/arrow:/tmp/arrow:Z \
  --volume ${OFFLINE_BUILD_DIR}/cachi2-output:/tmp/cachi2-output:Z \
  --volume ${OFFLINE_BUILD_DIR}/cachi2.env:/tmp/cachi2.env:Z \
  --network none \
  --tag arrow-builder \
  -f sdk/python/feast/infra/feature_servers/multicloud/Dockerfile.builder.arrow \
  --load offline_build/arrow

# pip builder
docker build \
  --tag pip-builder \
  -f sdk/python/feast/infra/feature_servers/multicloud/Dockerfile.builder.pip \
  --load sdk/python/feast/infra/feature_servers/multicloud

#git clone --branch v2.27.2 https://github.com/pydantic/pydantic-core ${OFFLINE_BUILD_DIR}/pydantic-core
#uv export --all-groups --format requirements-txt -o ${OFFLINE_BUILD_DIR}/pydantic-core/requirements.txt --project ${OFFLINE_BUILD_DIR}/pydantic-core -p 3.11

# ibis OFFLINE builder
docker build \
  --volume ${OFFLINE_BUILD_DIR}/cachi2-output:/tmp/cachi2-output:Z \
  --volume ${OFFLINE_BUILD_DIR}/cachi2.env:/tmp/cachi2.env:Z \
  --network none \
  --tag ibis-builder \
  -f sdk/python/feast/infra/feature_servers/multicloud/Dockerfile.builder.ibis \
  --load sdk/python/feast/infra/feature_servers/multicloud

# is this needed? check for reqs logs as feast builds
# maturin OFFLINE builder
#mkdir -p ${OFFLINE_BUILD_DIR}/cachi2-maturin
#git clone --branch v1.8.3 https://github.com/PyO3/maturin ${OFFLINE_BUILD_DIR}/maturin
#cachi2 fetch-deps cargo --source ${OFFLINE_BUILD_DIR}/maturin --output ${OFFLINE_BUILD_DIR}/cachi2-maturin
#cachi2 inject-files --for-output-dir /tmp/cachi2-maturin ${OFFLINE_BUILD_DIR}/cachi2-maturin
#docker build \
#  --volume ${OFFLINE_BUILD_DIR}/cachi2-maturin:/tmp/cachi2-maturin:Z \
#  --volume ${OFFLINE_BUILD_DIR}/cachi2-output:/tmp/cachi2-output:Z \
#  --volume ${OFFLINE_BUILD_DIR}/cachi2.env:/tmp/cachi2.env:Z \
#  --network none \
#  --tag maturin-builder \
#  -f sdk/python/feast/infra/feature_servers/multicloud/Dockerfile.builder.maturin \
#  --load ${OFFLINE_BUILD_DIR}/maturin

# feast OFFLINE builder
docker build \
  --volume ${OFFLINE_BUILD_DIR}/cachi2-output:/tmp/cachi2-output:Z \
  --volume ${OFFLINE_BUILD_DIR}/cachi2.env:/tmp/cachi2.env:Z \
  --network none \
  --tag feast:build \
  -f sdk/python/feast/infra/feature_servers/multicloud/Dockerfile.sdist \
  --load .

docker run --rm -ti feast:build feast version
