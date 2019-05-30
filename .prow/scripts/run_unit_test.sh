#!/usr/bin/env bash
set -e

# This script will run unit test for a specific Feast component:
# - core, ingestion, serving or cli
#
# This script includes the pre and post test scripts, such as
# - downloading maven cache repository
# - saving the test output report so it can be viewed with Spyglass in Prow

usage()
{
    echo "usage: run_unit_test.sh
    --component {core, ingestion, serving, cli}"
}

while [ "$1" != "" ]; do
  case "$1" in
      --component )       COMPONENT="$2";    shift;;
      * )                 usage; exit 1
  esac
  shift
done

if [[ ! ${COMPONENT} ]]; then 
  usage; exit 1; 
fi

. .prow/scripts/install_google_cloud_sdk.sh

if [[ ${COMPONENT} == "core" ]] || [[ ${COMPONENT} == "ingestion" ]] || [[ ${COMPONENT} == "serving" ]]; then

    .prow/scripts/prepare_maven_cache.sh --archive-uri gs://feast-templocation-kf-feast/.m2.tar --output-dir /root/
    mvn --projects ${COMPONENT} test
    TEST_EXIT_CODE=$?
    cp -r ${COMPONENT}/target/surefire-reports /logs/artifacts/surefire-reports

elif [[ ${COMPONENT} == "cli" ]]; then

    go get -u github.com/jstemmer/go-junit-report
    go test -v ./cli/feast/... 2>&1 | tee test_output
    TEST_EXIT_CODE=$?
    cat test_output | ${GOPATH}/bin/go-junit-report > ${ARTIFACTS}/unittest-cli-report.xml

elif [[ ${COMPONENT} == "python-sdk" ]]; then

    pip install -r test-requirements.txt
    pip install .
    pytest ./tests --junitxml=${ARTIFACTS}/unittest-pythonsdk-report.xml
    TEST_EXIT_CODE=$?

else
    usage; exit 1
fi

exit ${TEST_EXIT_CODE}
