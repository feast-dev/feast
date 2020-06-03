#!/usr/bin/env bash

apt-get -y install redis-server > /var/log/redis.install.log

mkdir 7000 7001 7002 7003 7004 7005
for i in {0..5} ; do
echo "port 700$i
cluster-enabled yes
cluster-config-file nodes-$i.conf
cluster-node-timeout 5000
appendonly yes" > 700$i/redis.conf
redis-server 700$i/redis.conf --daemonize yes
done
echo yes | redis-cli --cluster create 127.0.0.1:7000 127.0.0.1:7001 \
127.0.0.1:7002 127.0.0.1:7003 127.0.0.1:7004 127.0.0.1:7005 \
--cluster-replicas 1
