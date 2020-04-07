#!/usr/bin/env bash

set -e 
set -o pipefail

usage()
{
    echo "usage: publish-python-sdk.sh

    --directory-path  absolute path to the python package, this directory 
                      should contain 'setup.py' file

    --repository      the repository name where the package will be uploaded,
                      check your .pypirc configuration file for the list of 
                      valid repositories, usually it's 'pypi' or 'testpypi'
"
}

while [ "$1" != "" ]; do
  case "$1" in
      --directory-path )       DIRECTORY_PATH="$2";     shift;;
      --repository )           REPOSITORY="$2";         shift;;
      -h | --help )            usage;                   exit;; 
      * )                      usage;                   exit 1
  esac
  shift
done

if [ -z $DIRECTORY_PATH ]; then usage; exit 1; fi
if [ -z $REPOSITORY ]; then usage; exit 1; fi

ORIGINAL_DIR=$PWD
cd $DIRECTORY_PATH

echo "============================================================"
echo "Generating distribution archives"
echo "============================================================"
python3 -m pip install --user --upgrade setuptools wheel
python3 setup.py sdist bdist_wheel

echo "============================================================"
echo "Uploading distribution archives"
echo "============================================================"
python3 -m pip install --user --upgrade twine
python3 -m twine upload --repository $REPOSITORY dist/*

cd $ORIGINAL_DIR
