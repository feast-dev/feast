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
package feast.serving.service;

import feast.proto.serving.ServingAPIProto.FeatureReferenceV2;

public class FeatureUtil {

  /**
   * Accepts FeatureReferenceV2 object and returns its reference in String
   * "featuretable_name:feature_name".
   *
   * @param featureReference {@link FeatureReferenceV2}
   * @return String format of FeatureReferenceV2
   */
  public static String getFeatureReference(FeatureReferenceV2 featureReference) {
    String ref = featureReference.getFeatureName();
    if (!featureReference.getFeatureViewName().isEmpty()) {
      ref = featureReference.getFeatureViewName() + ":" + ref;
    }
    return ref;
  }

  public static FeatureReferenceV2 parseFeatureReference(String featureReference) {
    String[] tokens = featureReference.split(":", 2);
    return FeatureReferenceV2.newBuilder()
        .setFeatureViewName(tokens[0])
        .setFeatureName(tokens[1])
        .build();
  }
}
