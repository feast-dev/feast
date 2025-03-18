# to run -> source ./infra/scripts/cachi2.sh

# Get Feast project repository root directory
export PROJECT_ROOT_DIR=$(git rev-parse --show-toplevel)
alias cachi2='docker run --rm -ti -v "$PWD:$PWD:z" -w "$PWD" quay.io/konflux-ci/cachi2:f7a61b067f4446e4982d0e3b9545ce4aa0d8284f'

cd ${PROJECT_ROOT_DIR}

# comment out these 3 image builds out once they're working
#####################
docker build \
  --tag sdist-builder \
  -f sdk/python/feast/infra/feature_servers/multicloud/Dockerfile.builder \
  --load sdk/python/feast/infra/feature_servers/multicloud

docker build \
  --tag grpc-builder \
  -f sdk/python/feast/infra/feature_servers/multicloud/Dockerfile.builder.grpc \
  --load sdk/python/feast/infra/feature_servers/multicloud

docker build \
  --tag feast-sdist-builder \
  -f sdk/python/feast/infra/feature_servers/multicloud/Dockerfile.builder.arrow \
  --load sdk/python/feast/infra/feature_servers/multicloud
#####################

rm -rf ./cachi2-output ./cachi2.env
cachi2 fetch-deps \
  --output ./cachi2-output \
  '{
  "type": "pip",
  "path": ".",
  "requirements_files": ["sdk/python/feast/infra/feature_servers/multicloud/requirements.txt"],
  "requirements_build_files": ["sdk/python/requirements/py3.11-build-requirements.txt", "sdk/python/requirements/py3.11-pandas-requirements.txt", "sdk/python/requirements/py3.11-sdist-requirements.txt"],
  "allow_binary": "false"
}'

cachi2 generate-env ./cachi2-output -o ./cachi2.env --for-output-dir /tmp/cachi2-output

docker build \
  --volume "$(realpath ./cachi2-output)":/tmp/cachi2-output:Z \
  --volume "$(realpath ./cachi2.env)":/tmp/cachi2.env:Z \
  --network none \
  --tag feast:build \
  -f sdk/python/feast/infra/feature_servers/multicloud/Dockerfile.sdist \
  --load sdk/python/feast/infra/feature_servers/multicloud

docker run --rm -ti feast:build feast version
