/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
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
package com.gojek.feast;

import feast.serving.ServingAPIProto.FeatureSetRequest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

@SuppressWarnings("WeakerAccess")
public class RequestUtil {
  public static List<FeatureSetRequest> createFeatureSets(List<String> featureIds) {
    if (featureIds == null) {
      throw new IllegalArgumentException("featureIds cannot be null");
    }

    // featureSetMap is a map of pair of feature set name and version -> a list of feature names
    Map<Pair<String, Integer>, List<String>> featureSetMap = new HashMap<>();

    for (String featureId : featureIds) {
      String[] parts = featureId.split(":");
      if (parts.length < 3) {
        throw new IllegalArgumentException(
            String.format(
                "Feature id '%s' has invalid format. Expected format: <feature_set_name>:<version>:<feature_name>.",
                featureId));
      }
      String featureSetName = parts[0];
      int featureSetVersion;
      try {
        featureSetVersion = Integer.parseInt(parts[1]);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            String.format(
                "Feature id '%s' contains invalid version. Expected format: <feature_set_name>:<version>:<feature_name>.",
                parts[1]));
      }

      Pair<String, Integer> key = new ImmutablePair<>(featureSetName, featureSetVersion);
      if (!featureSetMap.containsKey(key)) {
        featureSetMap.put(key, new ArrayList<>());
      }
      String featureName = parts[2];
      featureSetMap.get(key).add(featureName);
    }

    return featureSetMap.entrySet().stream()
        .map(
            entry ->
                FeatureSetRequest.newBuilder()
                    .setName(entry.getKey().getKey())
                    .setVersion(entry.getKey().getValue())
                    .addAllFeatureNames(entry.getValue())
                    .build())
        .collect(Collectors.toList());
  }
}
