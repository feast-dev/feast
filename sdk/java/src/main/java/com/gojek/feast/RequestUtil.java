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
   * @param project specifies to the project set in parsed Feature Reference protos otherwise ""
   * @return List of parsed {@link FeatureReference} protos
   */
  public static List<FeatureReference> createFeatureRefs(
      List<String> featureRefStrings, String project) {
    if (featureRefStrings == null) {
      throw new IllegalArgumentException("featureRefs cannot be null");
    }

    List<FeatureReference.Builder> featureRefs =
        featureRefStrings.stream()
            .map(refStr -> parseFeatureRef(refStr, false))
            .collect(Collectors.toList());
    // apply project override if specified
    if (!project.isEmpty()) {
      featureRefs =
          featureRefs.stream().map(ref -> ref.setProject(project)).collect(Collectors.toList());
    }

    return featureRefs.stream().map(ref -> ref.build()).collect(Collectors.toList());
  }

  /**
   * Parse a feature reference proto builder from the given featureRefString
   *
   * @param featureRefString string feature reference to parse from.
   * @param ignoreProject whether to ignore project when parsing.
   * @return a parsed {@link FeatureReference.Builder}
   */
  public static FeatureReference.Builder parseFeatureRef(
      String featureRefString, boolean ignoreProject) {
    featureRefString = featureRefString.trim();
    if (featureRefString.isEmpty()) {
      throw new IllegalArgumentException("Cannot parse a empty feature reference");
    }
    FeatureReference.Builder featureRef = FeatureReference.newBuilder();

    // parse project if specified
    if (featureRefString.contains("/")) {
      if (ignoreProject) {
        String[] projectSplit = featureRefString.split("/");
        featureRefString = projectSplit[1];
      } else {
        throw new IllegalArgumentException(
            String.format("Unsupported feature reference: %s", featureRefString));
      }
    }

    // parse featureset if specified
    if (featureRefString.contains(":")) {
      String[] featureSetSplit = featureRefString.split(":");
      featureRef.setFeatureSetName(featureSetSplit[0]);
      featureRefString = featureSetSplit[1];
    }
    featureRef.setName(featureRefString);
    return featureRef;
  }

  /**
   * Render a feature reference as string.
   *
   * @param featureReference to render as string
   * @return string represenation of feature reference.
   */
  public static String renderFeatureRef(FeatureReference featureReference) {
    String refStr = "";
    // In protov3, unset string and int fields default to "" and 0 respectively
    if (!featureReference.getFeatureSetName().isEmpty()) {
      refStr += featureReference.getFeatureSetName() + ":";
    }
    refStr = refStr + featureReference.getName();

    return refStr;
  }
}
