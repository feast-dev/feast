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

import feast.proto.serving.ServingAPIProto.FeatureReference;
import java.util.List;
import java.util.stream.Collectors;

@SuppressWarnings("WeakerAccess")
public class RequestUtil {

  /**
   * Create feature references protos from given string feature reference.
   *
   * @param featureRefStrings to create Feature Reference protos from
   * @return List of parsed {@link FeatureReference} protos
   */
  public static List<FeatureReference> createFeatureRefs(List<String> featureRefStrings) {
    if (featureRefStrings == null) {
      throw new IllegalArgumentException("featureRefs cannot be null");
    }

    List<FeatureReference.Builder> featureRefs =
        featureRefStrings.stream()
            .map(refStr -> parseFeatureRef(refStr))
            .collect(Collectors.toList());

    return featureRefs.stream().map(ref -> ref.build()).collect(Collectors.toList());
  }

  /**
   * Parse a feature reference proto builder from the given featureRefString
   *
   * @param featureRefString string feature reference to parse from.
   * @return a parsed {@link FeatureReference.Builder}
   */
  public static FeatureReference.Builder parseFeatureRef(String featureRefString) {
    featureRefString = featureRefString.trim();
    if (featureRefString.isEmpty()) {
      throw new IllegalArgumentException("Cannot parse a empty feature reference");
    }
    if (featureRefString.contains("/")) {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported feature reference: Specifying project in string"
                  + " Feature References is not longer supported: %s",
              featureRefString));
    }

    FeatureReference.Builder featureRef = FeatureReference.newBuilder();
    // parse featureset if specified
    if (featureRefString.contains(":")) {
      String[] featureSetSplit = featureRefString.split(":");
      featureRef.setFeatureSet(featureSetSplit[0]);
      featureRefString = featureSetSplit[1];
    }
    featureRef.setName(featureRefString);
    return featureRef;
  }
}
