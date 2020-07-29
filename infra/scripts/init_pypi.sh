#!/usr/bin/env bash

mkdir -p /.config && mkdir -p /.config/pip
echo -e "[global]\nindex-url = https://$PYPI_USER:$PYPI_PWD@$PYPI_REPO\nextra-index-url =  https://pypi.org/simple/\n" > /.config/pip/pip.conf
export PIP_CONFIG_FILE=/.config/pip/pip.conf