#! /bin/bash

err_report() {
    ERRCODE=$?
    echo
    echo "Error on line $1"
    echo
    cat test_script.sh | head -n $1 | tail -n $[1-5]
    echo
    cat ${SCRIPTS_DIR}/output/last
    exit $ERRCODE
}

trap 'err_report $LINENO' ERR
set -x

rm -rf output
mkdir -p output
SCRIPTS_DIR=$(pwd)


# Step 1: version
{
feast version
} > ${SCRIPTS_DIR}/output/last

cat ${SCRIPTS_DIR}/output/last > ${SCRIPTS_DIR}/output/1
cat ${SCRIPTS_DIR}/output/1

# Step 2: init
{
feast init feature_repo
cd feature_repo
} > ${SCRIPTS_DIR}/output/last

cat ${SCRIPTS_DIR}/output/last > ${SCRIPTS_DIR}/output/2
cat ${SCRIPTS_DIR}/output/2

# Step 3: apply
{
feast apply
} > ${SCRIPTS_DIR}/output/last

cat ${SCRIPTS_DIR}/output/last > ${SCRIPTS_DIR}/output/3
cat ${SCRIPTS_DIR}/output/3

# Step 4: training
python ${SCRIPTS_DIR}/training.py > ${SCRIPTS_DIR}/output/last

cat ${SCRIPTS_DIR}/output/last > ${SCRIPTS_DIR}/output/4
cat ${SCRIPTS_DIR}/output/4

# Step 5: materialize
{
CURRENT_TIME=$(date -u +"%Y-%m-%dT%H:%M:%S")
feast materialize-incremental $CURRENT_TIME
} > ${SCRIPTS_DIR}/output/last

cat ${SCRIPTS_DIR}/output/last > ${SCRIPTS_DIR}/output/5
cat ${SCRIPTS_DIR}/output/5

# Step 6: predict
python ${SCRIPTS_DIR}/predict.py > ${SCRIPTS_DIR}/output/last

cat ${SCRIPTS_DIR}/output/last > ${SCRIPTS_DIR}/output/6
cat ${SCRIPTS_DIR}/output/6


python ${SCRIPTS_DIR}/validate_output.py ${SCRIPTS_DIR}/output/