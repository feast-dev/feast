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
import io.lettuce.core.KeyValue;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class RedisHashDecoder {

  /**
   * Converts all retrieved Redis Hash values based on EntityRows into {@link Feature}
   *
   * @param redisHashValues retrieved Redis Hash values based on EntityRows
   * @param byteToFeatureReferenceMap map to decode bytes back to FeatureReference
   * @return List of {@link Feature}
   * @throws InvalidProtocolBufferException
   */
  public static List<Optional<Feature>> retrieveFeature(
      List<KeyValue<byte[], byte[]>> redisHashValues,
      Map<String, ServingAPIProto.FeatureReferenceV2> byteToFeatureReferenceMap,
      String timestampPrefix)
      throws InvalidProtocolBufferException {
    List<Optional<Feature>> allFeatures = new ArrayList<>();
    Map<ServingAPIProto.FeatureReferenceV2, Optional<Feature.Builder>> allFeaturesBuilderMap =
        new HashMap<>();
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
              byteToFeatureReferenceMap.get(redisValueK.toString());
          ValueProto.Value featureValue = ValueProto.Value.parseFrom(redisValueV);

          Feature.Builder featureBuilder =
              Feature.builder().setFeatureReference(featureReference).setFeatureValue(featureValue);
          allFeaturesBuilderMap.put(featureReference, Optional.of(featureBuilder));
        }
      }
    }

    // Add timestamp to features
    for (Map.Entry<ServingAPIProto.FeatureReferenceV2, Optional<Feature.Builder>> entry :
        allFeaturesBuilderMap.entrySet()) {
      String timestampRedisHashKeyStr = timestampPrefix + ":" + entry.getKey().getFeatureTable();
      Timestamp curFeatureTimestamp = featureTableTimestampMap.get(timestampRedisHashKeyStr);

      Feature curFeature = entry.getValue().get().setEventTimestamp(curFeatureTimestamp).build();
      allFeatures.add(Optional.of(curFeature));
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
