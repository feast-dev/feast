#!/usr/bin/env bash

# This script will run unit test for a specific Feast component:
# - core, serving and [python/java/go]-sdk
#
# This script includes the pre and post test scripts, such as:
# - Download cached Maven packages for faster tests
# - Saving the test output report so it can be viewed with Spyglass in Prow.
#   By default, the configured log path is "/logs" and test artifacts should
#   be saved to "/logs/artifacts" directory.

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

if [[ ${COMPONENT} == "serving" ]]; then

  .prow/scripts/prepare_maven_cache.sh \
    --archive-uri gs://feast-templocation-kf-feast/.m2.tar --output-dir /root/

  mvn --define skipTests=true --projects serving install
  mvn --projects serving test

  TEST_EXIT_CODE=$?
  cp -r serving/target/surefire-reports /logs/artifacts/surefire-reports

elif [[ ${COMPONENT} == "core" ]]; then

  .prow/scripts/prepare_maven_cache.sh \
    --archive-uri gs://feast-templocation-kf-feast/.m2.tar --output-dir /root/

  # Core depends on Ingestion so they are tested together
  mvn --define skipTests=true --projects core,ingestion install
  mvn --projects core,ingestion test

  TEST_EXIT_CODE=$?
  cp -r core/target/surefire-reports /logs/artifacts/surefire-reports
  cp -r ingestion/target/surefire-reports /logs/artifacts/surefire-reports

elif [[ ${COMPONENT} == "java-sdk" ]]; then

  .prow/scripts/prepare_maven_cache.sh \
    --archive-uri gs://feast-templocation-kf-feast/.m2.tar --output-dir /root/

  # Core depends on Ingestion so they are tested together
  mvn --define skipTests=true --projects core,ingestion install
  mvn --projects core,ingestion test

  TEST_EXIT_CODE=$?
  cp -r core/target/surefire-reports /logs/artifacts/surefire-reports
  cp -r ingestion/target/surefire-reports /logs/artifacts/surefire-reports

elif [[ ${COMPONENT} == "python-sdk" ]]; then

  cd sdk/python
  pip install -r requirements-test.txt
  pip install .
  python -m pytest --junitxml=/logs/artifacts/unittest-pythonsdk-report.xml
  TEST_EXIT_CODE=$?

else
  usage; exit 1
fi

exit ${TEST_EXIT_CODE}
