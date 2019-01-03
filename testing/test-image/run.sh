#!/usr/bin/env bash

set -ex

git clone https://github.com/gojek/feast
cd feast
mvn test