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
package feast.storage.connectors.redis.common;

import com.google.common.hash.Hashing;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import feast.proto.serving.ServingAPIProto;
import feast.proto.types.ValueProto;
import feast.storage.api.retriever.Feature;
import feast.storage.api.retriever.ProtoFeature;
import io.lettuce.core.KeyValue;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class RedisHashDecoder {

  /**
   * Converts all retrieved Redis Hash values based on EntityRows into {@link Feature}
   *
   * @param redisHashValues retrieved Redis Hash values based on EntityRows
   * @param byteToFeatureReferenceMap map to decode bytes back to FeatureReference
   * @param timestampPrefix timestamp prefix
   * @return List of {@link Feature}
   * @throws InvalidProtocolBufferException if a protocol buffer exception occurs
   */
  public static List<Feature> retrieveFeature(
      List<KeyValue<byte[], byte[]>> redisHashValues,
      Map<byte[], ServingAPIProto.FeatureReferenceV2> byteToFeatureReferenceMap,
      String timestampPrefix)
      throws InvalidProtocolBufferException {
    List<Feature> allFeatures = new ArrayList<>();
    HashMap<ServingAPIProto.FeatureReferenceV2, ValueProto.Value> featureMap = new HashMap<>();
    Map<String, Timestamp> featureTableTimestampMap = new HashMap<>();

    for (KeyValue<byte[], byte[]> entity : redisHashValues) {
      if (entity.hasValue()) {
        byte[] redisValueK = entity.getKey();
        byte[] redisValueV = entity.getValue();

        // Decode data from Redis into Feature object fields
        if (new String(redisValueK).startsWith(timestampPrefix)) {
          Timestamp eventTimestamp = Timestamp.parseFrom(redisValueV);
          featureTableTimestampMap.put(new String(redisValueK), eventTimestamp);
        } else {
          ServingAPIProto.FeatureReferenceV2 featureReference =
              byteToFeatureReferenceMap.get(redisValueK);
          ValueProto.Value featureValue = ValueProto.Value.parseFrom(redisValueV);

          featureMap.put(featureReference, featureValue);
        }
      }
    }

    // Add timestamp to features
    for (Map.Entry<ServingAPIProto.FeatureReferenceV2, ValueProto.Value> entry :
        featureMap.entrySet()) {
      String timestampRedisHashKeyStr = timestampPrefix + ":" + entry.getKey().getFeatureTable();
      Timestamp curFeatureTimestamp = featureTableTimestampMap.get(timestampRedisHashKeyStr);

      ProtoFeature curFeature =
          new ProtoFeature(entry.getKey(), curFeatureTimestamp, entry.getValue());
      allFeatures.add(curFeature);
    }

    return allFeatures;
  }

  public static byte[] getTimestampRedisHashKeyBytes(
      ServingAPIProto.FeatureReferenceV2 featureReference, String timestampPrefix) {
    String timestampRedisHashKeyStr = timestampPrefix + ":" + featureReference.getFeatureTable();
    return timestampRedisHashKeyStr.getBytes();
  }

  public static byte[] getFeatureReferenceRedisHashKeyBytes(
      ServingAPIProto.FeatureReferenceV2 featureReference) {
    String delimitedFeatureReference =
        featureReference.getFeatureTable() + ":" + featureReference.getName();
    return Hashing.murmur3_32()
        .hashString(delimitedFeatureReference, StandardCharsets.UTF_8)
        .asBytes();
  }
}
