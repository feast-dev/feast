#!/bin/bash

sudo apt-get update 
sudo apt-get install -y curl build-essential tcl

wget http://download.redis.io/redis-stable.tar.gz
tar xvzf redis-stable.tar.gz
cd redis-stable

make install
redis-server --protected-mode no 