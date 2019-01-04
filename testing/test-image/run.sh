#!/usr/bin/env bash

set -e
printenv

if [ $# -eq 0 ]
then
    echo ""
    echo "please specify commands as arguments, for example \"make test\""
    echo ""
    exit 1
fi

echo ""
echo "cloning Feast repository..."
echo ""

git clone https://github.com/gojek/feast
cd feast

if [ -n "${PULL_NUMBER}" ] && [ "$JOB_TYPE" = "presubmit" ] 
then
    echo ""
    echo "fetching PR ${PULL_NUMBER}..."
    echo ""
    git fetch origin pull/"${PULL_NUMBER}"/head:pull_"${PULL_NUMBER}"

    echo ""
    echo "checking out PR ${PULL_NUMBER}..."
    echo ""
    git checkout pull_"${PULL_NUMBER}"
fi

echo "sha:"
git rev-parse HEAD
echo ""
echo "running unit tests"
echo ""

$@