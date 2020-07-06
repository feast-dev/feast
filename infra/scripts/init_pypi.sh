#!/usr/bin/env bash

mkdir /.config && mkdir /.config/pip
echo -e "[global]\nindex-url = https://$PYPI_USER:$PYPI_PWD@pypi.prod.konnekt.us/simple/\nextra-index-url =  https://pypi.org/simple/\n" > /.config/pip/pip.conf
export PIP_CONFIG_FILE=/.config/pip/pip.conf
pip install feast==0.5.1.dev471+g8c76c49d