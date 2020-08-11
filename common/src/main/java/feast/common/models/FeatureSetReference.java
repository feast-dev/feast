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

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * FeatureSetReference is key that uniquely defines specific version of FeatureSet or FeatureSetSpec
 */
@Data
@AllArgsConstructor
public class FeatureSetReference implements Serializable {
  public static String PROJECT_DEFAULT_NAME = "default";

  /* Name of project to which this featureSet is assigned */
  private String projectName;
  /* Name of FeatureSet */
  private String featureSetName;
  /* Version of FeatureSet */
  private Integer version;

  // Empty constructor required for Avro decoding.
  @SuppressWarnings("unused")
  public FeatureSetReference() {}

  public static FeatureSetReference of(String projectName, String featureSetName, Integer version) {
    projectName = projectName.isEmpty() ? PROJECT_DEFAULT_NAME : projectName;
    return new FeatureSetReference(projectName, featureSetName, version);
  }

  public static FeatureSetReference of(String projectName, String featureSetName) {
    return FeatureSetReference.of(projectName, featureSetName, -1);
  }

  public String getReference() {
    return String.format("%s/%s", getProjectName(), getFeatureSetName());
  }
}
