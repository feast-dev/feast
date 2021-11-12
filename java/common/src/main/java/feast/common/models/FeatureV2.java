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
package feast.common.models;

import feast.proto.serving.ServingAPIProto.FeatureReferenceV2;

public class FeatureV2 {

  /**
   * Accepts FeatureReferenceV2 object and returns its reference in String
   * "featuretable_name:feature_name".
   *
   * @param featureReference {@link FeatureReferenceV2}
   * @return String format of FeatureReferenceV2
   */
  public static String getFeatureStringRef(FeatureReferenceV2 featureReference) {
    String ref = featureReference.getName();
    if (!featureReference.getFeatureTable().isEmpty()) {
      ref = featureReference.getFeatureTable() + ":" + ref;
    }
    return ref;
  }

  /**
   * Accepts either a feature reference of the form "featuretable_name:feature_name" or just a
   * feature name, and returns just the feature name. For example, given either
   * "driver_hourly_stats:conv_rate" or "conv_rate", "conv_rate" would be returned.
   *
   * @param featureReference {String}
   * @return Base feature name of the feature reference
   */
  public static String getFeatureName(String featureReference) {
    String[] tokens = featureReference.split(":", 2);
    return tokens[tokens.length - 1];
  }
}
