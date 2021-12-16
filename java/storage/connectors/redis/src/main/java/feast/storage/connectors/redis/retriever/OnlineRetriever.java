/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package feast.storage.connectors.redis.retriever;

import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.proto.serving.ServingAPIProto;
import feast.proto.storage.RedisProto;
import feast.storage.api.retriever.Feature;
import feast.storage.api.retriever.OnlineRetrieverV2;
import feast.storage.connectors.redis.common.RedisHashDecoder;
import feast.storage.connectors.redis.common.RedisKeyGenerator;
import io.grpc.Status;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import io.lettuce.core.Value;
import org.slf4j.Logger;

public class OnlineRetriever implements OnlineRetrieverV2 {

    private static final Logger log = org.slf4j.LoggerFactory.getLogger(OnlineRetriever.class);

    private static final String timestampPrefix = "_ts";
    private final RedisClientAdapter redisClientAdapter;
    private final EntityKeySerializer keySerializer;

    public OnlineRetriever(RedisClientAdapter redisClientAdapter, EntityKeySerializer keySerializer) {
        this.redisClientAdapter = redisClientAdapter;
        this.keySerializer = keySerializer;
    }

    @Override
    public List<List<Feature>> getOnlineFeatures(
        String project,
        List<ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow> entityRows,
        List<ServingAPIProto.FeatureReferenceV2> featureReferences,
        List<String> entityNames) {

        List<RedisProto.RedisKeyV2> redisKeys = RedisKeyGenerator.buildRedisKeys(project, entityRows);
        List<List<Feature>> features = getFeaturesFromRedis(redisKeys, featureReferences);

        return features;
    }

    private List<List<Feature>> getFeaturesFromRedis(
        List<RedisProto.RedisKeyV2> redisKeys,
        List<ServingAPIProto.FeatureReferenceV2> featureReferences) {
        List<List<Feature>> features = new ArrayList<>();
        // To decode bytes back to Feature Reference
        Map<ByteBuffer, ServingAPIProto.FeatureReferenceV2> byteToFeatureReferenceMap = new HashMap<>();

        // Serialize using proto
        List<byte[]> binaryRedisKeys =
            redisKeys.stream().map(this.keySerializer::serialize).collect(Collectors.toList());

        List<byte[]> retrieveFields = new ArrayList<>();
        featureReferences.stream()
            .forEach(
                featureReference -> {

                    // eg. murmur(<featuretable_name:feature_name>)
                    byte[] featureReferenceBytes =
                        RedisHashDecoder.getFeatureReferenceRedisHashKeyBytes(featureReference);
                    retrieveFields.add(featureReferenceBytes);
                    byteToFeatureReferenceMap.put(
                        ByteBuffer.wrap(featureReferenceBytes), featureReference);
                });

        featureReferences.stream()
            .map(ServingAPIProto.FeatureReferenceV2::getFeatureTable)
            .distinct()
            .forEach(
                table -> {
                    // eg. <_ts:featuretable_name>
                    byte[] featureTableTsBytes =
                        RedisHashDecoder.getTimestampRedisHashKeyBytes(table, timestampPrefix);

                    retrieveFields.add(featureTableTsBytes);
                }
            );

        List<Map<byte[], byte[]>> results;

        long start = System.currentTimeMillis();
        // Perform a series of independent calls
        if (retrieveFields.size() < 20) {
            byte[][] retrieveFieldsByteArray =
                retrieveFields.toArray(new byte[0][]);

            List<RedisFuture<List<KeyValue<byte[], byte[]>>>> futures = Lists.newArrayList();
            for (byte[] binaryRedisKey : binaryRedisKeys) {
                // Access redis keys and extract features
                futures.add(redisClientAdapter.hmget(binaryRedisKey, retrieveFieldsByteArray));
            }

            // Write all commands to the transport layer
            //redisClientAdapter.flushCommands();

            results = futures.stream().map(f -> {
                try {
                    return f.get().stream()
                        .filter(Value::hasValue)
                        .collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue));
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException();
                }
            }).collect(Collectors.toList());
        } else {
            List<RedisFuture<Map<byte[], byte[]>>> futures = Lists.newArrayList();
            for (byte[] binaryRedisKey : binaryRedisKeys) {
                futures.add(redisClientAdapter.hgetall(binaryRedisKey));
            }

            results = futures.stream().map(f -> {
                try {
                    return f.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException();
                }
            }).collect(Collectors.toList());
        }
        System.out.printf("Redis %d\n", System.currentTimeMillis() - start);

        results.forEach(
            redisValuesList -> {
                try {
                    List<Feature> curRedisKeyFeatures =
                        RedisHashDecoder.retrieveFeature(
                            redisValuesList, byteToFeatureReferenceMap, timestampPrefix);
                    features.add(curRedisKeyFeatures);
                } catch (InvalidProtocolBufferException e) {
                    throw Status.UNKNOWN
                        .withDescription("Unexpected error when pulling data from from Redis.")
                        .withCause(e)
                        .asRuntimeException();
                }
            });
        return features;
    }
}
