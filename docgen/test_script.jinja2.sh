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

{% for step in steps %}
# Step {{loop.index}}: {{step.name}}
{% if step.command -%}
{
{{ step.command | trim() }}
} > ${SCRIPTS_DIR}/output/last
{% else -%}
python ${SCRIPTS_DIR}/{{ step.python_script }} > ${SCRIPTS_DIR}/output/last
{% endif %}
cat ${SCRIPTS_DIR}/output/last > ${SCRIPTS_DIR}/output/{{loop.index}}
cat ${SCRIPTS_DIR}/output/{{loop.index}}
{% endfor %}

python ${SCRIPTS_DIR}/validate_output.py ${SCRIPTS_DIR}/output/