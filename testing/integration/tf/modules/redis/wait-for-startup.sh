#!/bin/bash

INSTANCE_ID=$1
HOST=$2
echo "Waiting for startup script in instance $INSTANCE_ID to complete..."

current_status=$((printf "PING\r\n";) | nc -w1 $HOST 6379; echo $?)

while [[ "$current_status" -eq "1" ]]; do
    printf "."
    sleep 5
    current_status=$((printf "PING\r\n";) | nc -w1 $HOST 6379; echo $?)
done
