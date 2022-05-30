~/grpcurl/grpcurl -plaintext -proto feast/serving/ServingService.proto -format json -d @ 127.0.0.1:6566 feast.serving.ServingService.GetOnlineFeatures < input.json
