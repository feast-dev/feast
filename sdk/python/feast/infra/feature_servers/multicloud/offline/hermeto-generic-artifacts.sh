#!/usr/bin/env bash

# modified from existing arrow script -
# https://github.com/apache/arrow/blob/apache-arrow-17.0.0/cpp/thirdparty/download_dependencies.sh

set -eu

APACHE_ARROW_VERSION="17.0.0"
APACHE_ARCHIVE_SHA256_CHECKSUM="8379554d89f19f2c8db63620721cabade62541f47a4e706dfb0a401f05a713ef"

SUBSTRAIT_VERSION="0.44.0"
SUBSTRAIT_ARCHIVE_SHA256_CHECKSUM="f989a862f694e7dbb695925ddb7c4ce06aa6c51aca945105c075139aed7e55a2"

MILVUS_LITE_VERSION="2.4.12"
MILVUS_LITE_ARCHIVE_SHA256_CHECKSUM="334037ebbab60243b5d8b43d54ca2f835d81d48c3cda0c6a462605e588deb05d"

IBIS_VERSION="9.5.0"
IBIS_ARCHIVE_SHA256_CHECKSUM="145fe30d94f111cff332580c275ce77725c5ff7086eede93af0b371649d009c0"

PROJECT_ROOT_DIR=$(git rev-parse --show-toplevel)
DESTDIR=${PROJECT_ROOT_DIR}/sdk/python/feast/infra/feature_servers/multicloud/offline
artifacts_file=${DESTDIR}/artifacts.lock.yaml

set_dependency() {
  local url=$1
  local hash=$2
  local file=$3

  echo "  - download_url: \"${url}\"" >> ${artifacts_file}
  echo "    checksum: \"sha256:${hash}\"" >>  ${artifacts_file}
  echo "    filename: \"${file}\"" >>  ${artifacts_file}
}

main() {
  ARROW_VERSION_TAG=apache-arrow-${APACHE_ARROW_VERSION}
  ARROW_ARCHIVE=${ARROW_VERSION_TAG}.tar.gz

  versions_file=${DESTDIR}/arrow-thirdparty-offline-versions
  wget https://raw.githubusercontent.com/apache/arrow/refs/tags/${ARROW_VERSION_TAG}/cpp/thirdparty/versions.txt -O ${versions_file}

  # Load `DEPENDENCIES` variable.
  hash_suffix="_BUILD_SHA256_CHECKSUM"
  while IFS= read -r line; do
    if [[ "$line" == *"${hash_suffix}"* ]]; then
      export "$line"
    fi
  done < ${versions_file}

  echo '---
metadata:
  version: "1.0"
artifacts:' > ${artifacts_file}

  set_dependency "https://files.pythonhosted.org/packages/44/43/b3f6e9defd1f3927b972beac7abe3d5b4a3bdb287e3bad69618e2e76cf0a/milvus_lite-${MILVUS_LITE_VERSION}-py3-none-manylinux2014_x86_64.whl" "${MILVUS_LITE_ARCHIVE_SHA256_CHECKSUM}" "milvus_lite-${MILVUS_LITE_VERSION}-py3-none-manylinux2014_x86_64.whl"
  set_dependency "https://files.pythonhosted.org/packages/dd/a9/899888a3b49ee07856a0bab673652a82ea89999451a51fba4d99e65868f7/ibis_framework-${IBIS_VERSION}-py3-none-any.whl" "${IBIS_ARCHIVE_SHA256_CHECKSUM}" "ibis_framework-${IBIS_VERSION}-py3-none-any.whl"
  set_dependency "https://github.com/apache/arrow/archive/${ARROW_ARCHIVE}" "${APACHE_ARCHIVE_SHA256_CHECKSUM}" "${ARROW_ARCHIVE}"

  source ${versions_file}
  for ((i = 0; i < ${#DEPENDENCIES[@]}; i++)); do
    local dep_packed=${DEPENDENCIES[$i]}

    # Unpack each entry of the form "$home_var $tar_out $dep_url"
    IFS=" " read -r dep_url_var dep_tar_name dep_url <<< "${dep_packed}"

    dep_hash_var=${dep_url_var%"_URL"}"${hash_suffix}"

    set_dependency "${dep_url}" "${!dep_hash_var}" "${dep_tar_name}"
  done

  # set substrait manually until using an arrow version w/ this fix - https://github.com/apache/arrow/pull/46191
  substrait_tar_name="substrait-${SUBSTRAIT_VERSION}.tar.gz"
  set_dependency "https://github.com/substrait-io/substrait/archive/v${SUBSTRAIT_VERSION}.tar.gz" "${SUBSTRAIT_ARCHIVE_SHA256_CHECKSUM}" ${substrait_tar_name}
  
  cat ${artifacts_file}
}

main
