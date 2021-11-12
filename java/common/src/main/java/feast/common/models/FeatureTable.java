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

import feast.proto.core.FeatureTableProto.FeatureTableSpec;
import feast.proto.serving.ServingAPIProto.FeatureReferenceV2;

public class FeatureTable {

  /**
   * Accepts FeatureTableSpec object and returns its reference in String
   * "project/featuretable_name".
   *
   * @param project project name
   * @param featureTableSpec {@link FeatureTableSpec}
   * @return String format of FeatureTableReference
   */
  public static String getFeatureTableStringRef(String project, FeatureTableSpec featureTableSpec) {
    return String.format("%s/%s", project, featureTableSpec.getName());
  }

  /**
   * Accepts FeatureReferenceV2 object and returns its reference in String
   * "project/featuretable_name".
   *
   * @param project project name
   * @param featureReference {@link FeatureReferenceV2}
   * @return String format of FeatureTableReference
   */
  public static String getFeatureTableStringRef(
      String project, FeatureReferenceV2 featureReference) {
    return String.format("%s/%s", project, featureReference.getFeatureTable());
  }
}
