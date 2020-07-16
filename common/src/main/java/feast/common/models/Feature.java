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

import feast.proto.serving.ServingAPIProto.FeatureReference;

public class Feature {

  /**
   * Accepts FeatureReference object and returns its reference in String
   * "featureset_name:feature_name".
   *
   * @param featureReference {@link FeatureReference}
   * @return String format of FeatureReference
   */
  public static String getFeatureStringRef(FeatureReference featureReference) {
    String ref = featureReference.getName();
    if (!featureReference.getFeatureSet().isEmpty()) {
      ref = featureReference.getFeatureSet() + ":" + ref;
    }
    return ref;
  }

  /**
   * Accepts FeatureReference object and returns its reference with project included in String, eg.
   * "project/featureset_name:feature_name".
   *
   * @param featureReference {@link FeatureReference}
   * @return String format of FeatureReference
   */
  public static String getFeatureStringWithProjectRef(FeatureReference featureReference) {
    String ref = featureReference.getName();
    if (!featureReference.getFeatureSet().isEmpty()) {
      ref = featureReference.getFeatureSet() + ":" + ref;
    }
    if (!featureReference.getProject().isEmpty()) {
      ref = featureReference.getProject() + "/" + ref;
    }
    return ref;
  }
}
