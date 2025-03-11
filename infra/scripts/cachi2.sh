# to run -> source ./infra/scripts/cachi2.sh
# requires uv

# Get Feast project repository root directory
export PROJECT_ROOT_DIR=$(git rev-parse --show-toplevel)
cd ${PROJECT_ROOT_DIR}

# arrow builder
#docker build \
#  --tag arrow-builder \
#  -f sdk/python/feast/infra/feature_servers/multicloud/Dockerfile.builder.arrow \
#  --load sdk/python/feast/infra/feature_servers/multicloud

############################

alias cachi2='docker run --rm -ti -v "$PWD:$PWD:z" -w "$PWD" quay.io/konflux-ci/cachi2:f7a61b067f4446e4982d0e3b9545ce4aa0d8284f'

rm -rf ./cachi2-output ./cachi2.env ./pydantic-core ./maturin ./ahash
#git clone --branch v0.8.11 https://github.com/tkaitchuck/ahash
#rm -rf ./cachi2-output
#cd ahash
#cargo generate-lockfile
#cd ${PROJECT_ROOT_DIR}
#cachi2 fetch-deps cargo --source ahash
#cachi2 inject-files --for-output-dir /tmp/cachi2-output cachi2-output
## ahash builder w/ cargo install
#docker build \
#  --network none \
#  --tag ahash-builder \
#  -f sdk/python/feast/infra/feature_servers/multicloud/Dockerfile.builder.ahash \
#  --load ahash

#rm -rf ./cachi2-output
#git clone --branch v1.8.3 https://github.com/PyO3/maturin
#cachi2 fetch-deps cargo --source maturin
#cachi2 inject-files --for-output-dir /tmp/cachi2-output cachi2-output
## maturin builder w/ cargo install
#docker build \
#  --volume "$(realpath ./cachi2-output)":/tmp/cachi2-output:Z \
#  --network none \
#  --tag maturin-builder \
#  -f sdk/python/feast/infra/feature_servers/multicloud/Dockerfile.builder.maturin \
#  --load maturin

#rm -rf ./cachi2-output
#git clone --branch v2.27.2 https://github.com/pydantic/pydantic-core
#cachi2 fetch-deps cargo --source pydantic-core
#uv export --all-groups --format requirements-txt -o pydantic-core/requirements.txt --project pydantic-core -p 3.11
#cachi2 inject-files --for-output-dir /tmp/cachi2-output cachi2-output
cachi2 fetch-deps \
  --output ./cachi2-output \
  '{
  "type": "pip",
  "path": ".",
  "requirements_files": [
"sdk/python/feast/infra/feature_servers/multicloud/requirements.txt"
],
  "requirements_build_files": [
"sdk/python/requirements/py3.11-sdist-requirements.txt",
"sdk/python/requirements/py3.11-pandas-requirements.txt"
],
  "allow_binary": "false"
}'
#"pydantic-core/requirements.txt",
#"sdk/python/requirements/py3.11-pydantic-requirements.txt"
cachi2 generate-env ./cachi2-output -o ./cachi2.env --for-output-dir /tmp/cachi2-output

## pydantic-core builder w/ pip install
#docker build \
#  --volume "$(realpath ./cachi2-output)":/tmp/cachi2-output:Z \
#  --volume "$(realpath ./cachi2.env)":/tmp/cachi2.env:Z \
#  --network none \
#  --tag feast-sdist-builder \
#  -f sdk/python/feast/infra/feature_servers/multicloud/Dockerfile.builder.pydantic-core \
#  --load pydantic-core

# feast builder
docker build \
  --volume "$(realpath ./cachi2-output)":/tmp/cachi2-output:Z \
  --volume "$(realpath ./cachi2.env)":/tmp/cachi2.env:Z \
  --network none \
  --tag feast:build \
  -f sdk/python/feast/infra/feature_servers/multicloud/Dockerfile.sdist \
  --load .

docker run --rm -ti feast:build feast version
