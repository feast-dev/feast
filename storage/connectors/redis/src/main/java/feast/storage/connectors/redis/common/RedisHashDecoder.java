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

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import feast.proto.serving.ServingAPIProto;
import feast.proto.types.ValueProto;
import feast.storage.api.retriever.Feature;
import io.lettuce.core.KeyValue;
import java.util.*;

public class RedisHashDecoder {

  /**
   * Converts all retrieved Redis Hash values based on EntityRows into {@link Feature}
   *
   * @param redisHashValues retrieved Redis Hash values based on EntityRows
   * @param isTimestampMap map to determine if Redis Hash key is a timestamp field
   * @param byteToFeatureReferenceMap map to decode bytes back to FeatureReference
   * @return List of {@link Feature}
   * @throws InvalidProtocolBufferException
   */
  public static List<Optional<Feature>> retrieveFeature(
      List<KeyValue<byte[], byte[]>> redisHashValues,
      Map<String, Boolean> isTimestampMap,
      Map<String, ServingAPIProto.FeatureReferenceV2> byteToFeatureReferenceMap,
      String timestampPrefix)
      throws InvalidProtocolBufferException {
    List<Optional<Feature>> allFeatures = new ArrayList<>();
    Map<ServingAPIProto.FeatureReferenceV2, Optional<Feature.Builder>> allFeaturesBuilderMap =
        new HashMap<>();
    Map<String, Timestamp> featureTableTimestampMap = new HashMap<>();

    for (int i = 0; i < redisHashValues.size(); i++) {
      if (redisHashValues.get(i).hasValue()) {
        byte[] redisValueK = redisHashValues.get(i).getKey();
        byte[] redisValueV = redisHashValues.get(i).getValue();

        // Decode data from Redis into Feature object fields
        if (isTimestampMap.get(Arrays.toString(redisValueK))) {
          Timestamp eventTimestamp = Timestamp.parseFrom(redisValueV);
          featureTableTimestampMap.put(Arrays.toString(redisValueK), eventTimestamp);
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
    if (allFeaturesBuilderMap.size() > 0) {
      for (Map.Entry<ServingAPIProto.FeatureReferenceV2, Optional<Feature.Builder>> entry :
          allFeaturesBuilderMap.entrySet()) {
        byte[] timestampFeatureTableHashKeyBytes =
            RedisHashDecoder.getTimestampRedisHashKeyBytes(entry.getKey(), timestampPrefix);
        Timestamp curFeatureTimestamp =
            featureTableTimestampMap.get(Arrays.toString(timestampFeatureTableHashKeyBytes));
        Feature curFeature = entry.getValue().get().setEventTimestamp(curFeatureTimestamp).build();
        allFeatures.add(Optional.of(curFeature));
      }
    }
    return allFeatures;
  }

  public static byte[] getTimestampRedisHashKeyBytes(
      ServingAPIProto.FeatureReferenceV2 featureReference, String timestampPrefix) {
    String timestampRedisHashKeyStr = timestampPrefix + ":" + featureReference.getFeatureTable();
    return timestampRedisHashKeyStr.getBytes();
  }
}
