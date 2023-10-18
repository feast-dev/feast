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
package feast.serving.connectors.redis.common;

import com.google.common.hash.Hashing;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import feast.proto.serving.ServingAPIProto;
import feast.proto.types.ValueProto;
import feast.serving.connectors.Feature;
import feast.serving.connectors.ProtoFeature;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class RedisHashDecoder {

  /**
   * Converts all retrieved Redis Hash values based on EntityRows into {@link Feature}
   *
   * @param redisHashValues retrieved Redis Hash values based on EntityRows
   * @param byteToFeatureIdxMap map to decode bytes back to FeatureReference
   * @param timestampPrefix timestamp prefix
   * @return Map of {@link ServingAPIProto.FeatureReferenceV2} to {@link Feature}
   */
  public static List<Feature> retrieveFeature(
      Map<byte[], byte[]> redisHashValues,
      Map<ByteBuffer, Integer> byteToFeatureIdxMap,
      List<ServingAPIProto.FeatureReferenceV2> featureReferences,
      String timestampPrefix) {
    Map<String, Timestamp> featureTableTimestampMap =
        redisHashValues.entrySet().stream()
            .filter(e -> new String(e.getKey()).startsWith(timestampPrefix))
            .collect(
                Collectors.toMap(
                    e -> new String(e.getKey()).substring(timestampPrefix.length() + 1),
                    e -> {
                      try {
                        return Timestamp.parseFrom(e.getValue());
                      } catch (InvalidProtocolBufferException ex) {
                        throw new RuntimeException(
                            "Couldn't parse timestamp proto while pulling data from Redis");
                      }
                    }));
    List<Feature> results = new ArrayList<>(Collections.nCopies(featureReferences.size(), null));

    for (Map.Entry<byte[], byte[]> entry : redisHashValues.entrySet()) {
      Integer featureIdx = byteToFeatureIdxMap.get(ByteBuffer.wrap(entry.getKey()));
      if (featureIdx == null) {
        continue;
      }

      ValueProto.Value v;
      try {
        v = ValueProto.Value.parseFrom(entry.getValue());
      } catch (InvalidProtocolBufferException ex) {
        throw new RuntimeException(
            "Couldn't parse feature value proto while pulling data from Redis");
      }
      results.set(
          featureIdx,
          new ProtoFeature(
              featureReferences.get(featureIdx),
              featureTableTimestampMap.get(featureReferences.get(featureIdx).getFeatureViewName()),
              v));
    }

    return results;
  }

  public static byte[] getTimestampRedisHashKeyBytes(String featureTable, String timestampPrefix) {
    String timestampRedisHashKeyStr = timestampPrefix + ":" + featureTable;
    return timestampRedisHashKeyStr.getBytes();
  }

  public static byte[] getFeatureReferenceRedisHashKeyBytes(
      ServingAPIProto.FeatureReferenceV2 featureReference) {
    String delimitedFeatureReference =
        featureReference.getFeatureViewName() + ":" + featureReference.getFeatureName();
    return Hashing.murmur3_32()
        .hashString(delimitedFeatureReference, StandardCharsets.UTF_8)
        .asBytes();
  }
}
