#!/bin/bash

curl -X POST http://localhost:8081/get-online-features -d '{"features":{"val":["driver_hourly_stats:conv_rate","driver_hourly_stats:acc_rate","driver_hourly_stats:avg_daily_trips"]},"entities":{"driver_id":{"val":[{"int64Val":"1001"},{"int64Val":"1002"},{"int64Val":"1003"}]}},"fullFeatureNames":true}'


