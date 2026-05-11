#!/bin/bash
set -Eeuo pipefail
trap 'echo "[prebuild-power] failed at line $LINENO"; exit 1' ERR
shopt -s dotglob nullglob

PYTHON_VERSION=3.12
WORKDIR=$(pwd)
CMAKE_VERSION=3.30.5
CMAKE_REQUIRED_VERSION=3.30.5

dnf install -y make cmake ninja-build libomp-devel \
               git python${PYTHON_VERSION} python${PYTHON_VERSION}-devel python${PYTHON_VERSION}-pip \
               openssl openssl-devel zlib-devel libuuid-devel 

export CC=gcc
export CXX=g++
export CXXFLAGS="-std=c++17 -mcmodel=medium -ffunction-sections"
export CFLAGS="-mcmodel=medium -ffunction-sections"
export LDFLAGS="-Wl,--stub-group-size=0x00002000 -Wl,--gc-sections"

# Ensure CXXFLAGS and LINKFLAGS are initialized
: "${CMAKE_ARGS:=""}"
: "${CXXFLAGS:=""}"
: "${CFLAGS:=""}"
: "${LINKFLAGS:=""}"

# Installing Python build dependencies
# fix pandas build
python${PYTHON_VERSION} -m pip install pandas==2.3.3 --extra-index-url https://wheels.developerfirst.ibm.com/ppc64le/linux/+simple/
python${PYTHON_VERSION} -m pip install build wheel 'setuptools<78' ninja pybind11 numpy setuptools_scm Cython

# Directory to collect built wheels
mkdir -p /wheelhouse

#######################################################
# Build DuckDB (Python package)
#######################################################
echo "Entering DuckDB source directory..."
git clone https://github.com/duckdb/duckdb.git
cd duckdb
git checkout v1.1.3
cd tools/pythonpkg
export SETUPTOOLS_SCM_PRETEND_VERSION=1.1.3
python${PYTHON_VERSION} -m build --wheel --no-isolation
# Cleanup
unset SETUPTOOLS_SCM_PRETEND_VERSION
ls dist/*.whl >/dev/null
cp -v dist/*.whl /wheelhouse/
cd $WORKDIR

#######################################################
# Build gRPC  (Python package)
#######################################################
echo "Building grpcio..."
export GRPC_PYTHON_BUILD_SYSTEM_OPENSSL=1
pip install grpcio==1.62.3

#######################################################
# Build Pyarrow  (Python package)
#######################################################
echo "Entering Pyarrow source directory..."
git clone https://github.com/apache/arrow.git
cd arrow
git checkout apache-arrow-22.0.0
git submodule update --init --recursive
cd cpp
mkdir -p release && cd release
cmake -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=/usr/local \
      -DARROW_PYTHON=ON \
      -DARROW_PARQUET=ON \
      -DARROW_ORC=ON \
      -DARROW_FILESYSTEM=ON \
      -DARROW_FLIGHT=ON \
      -DARROW_WITH_LZ4=ON \
      -DARROW_WITH_ZSTD=ON \
      -DARROW_WITH_SNAPPY=ON \
      -DARROW_JSON=ON \
      -DARROW_CSV=ON \
      -DARROW_DATASET=ON \
      -DARROW_S3=ON \
      -DARROW_BUILD_TESTS=OFF \
      -DARROW_SUBSTRAIT=ON \
      -DProtobuf_SOURCE=BUNDLED \
      -DARROW_DEPENDENCY_SOURCE=BUNDLED \
    ..
make -j$(nproc)
make install
cd ../../python
export BUILD_TYPE=release
python${PYTHON_VERSION} setup.py build_ext --build-type=$BUILD_TYPE --bundle-arrow-cpp bdist_wheel
ls dist/*.whl >/dev/null
cp -v dist/*.whl /wheelhouse/
cd ../../..

#######################################################
# Build Milvus-Lite  (Python package)
#######################################################
echo "Building milvus-lite..."
dnf install -y perl ncurses-devel wget openblas-devel cargo gcc gcc-c++ libstdc++-static which libaio \
               libtool m4 autoconf automake zlib-devel libffi-devel scl-utils xz

python${PYTHON_VERSION} -m pip install conan==1.64.1 setuptools==70.0.0

git clone -b v2.4.12 https://github.com/milvus-io/milvus-lite.git
cd milvus-lite/python
git submodule update --init --recursive
python${PYTHON_VERSION} -m pip install -v -e .
