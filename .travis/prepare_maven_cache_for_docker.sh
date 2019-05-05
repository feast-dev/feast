#!/usr/bin/env bash

# This works because Dockerfile for core and serving assumes Maven repository 
# is located at .m2 directory inside Feast repository root directory
cp -r $HOME/.m2 ${TRAVIS_BUILD_DIR}/.m2