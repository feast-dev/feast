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
package feast.core.model;

import javax.persistence.EmbeddedId;
import javax.persistence.MappedSuperclass;
import lombok.Getter;
import lombok.Setter;

// A field in a feature set, which may or may not contain value constraints
// for validation purposes.
@Getter
@Setter
@MappedSuperclass
public abstract class Field {
  @EmbeddedId private FieldId id;

  // Type of the field
  private String type;

  // Presence constraints (refer to proto feast.core.FeatureSet.FeatureSpec)
  // Only one of them can be set.
  private byte[] presence;
  private byte[] groupPresence;

  // Shape type (refer to proto feast.core.FeatureSet.FeatureSpec)
  // Only one of them can be set.
  private byte[] shape;
  private byte[] valueCount;

  // Domain info for the values (refer to proto feast.core.FeatureSet.FeatureSpec)
  // Only one of them can be set.
  private String domain;
  private byte[] intDomain;
  private byte[] floatDomain;
  private byte[] stringDomain;
  private byte[] boolDomain;
  private byte[] structDomain;
  private byte[] naturalLanguageDomain;
  private byte[] imageDomain;
  private byte[] midDomain;
  private byte[] urlDomain;
  private byte[] timeDomain;
  private byte[] timeOfDayDomain;

  public void setName(String name) {
    this.id.setName(name);
  }

  public void setProject(String project) {
    this.id.setProject(project);
  }

  public void setVersion(int version) {
    this.id.setVersion(version);
  }

  public void setFeatureSet(String featureSet) {
    this.id.setFeatureSet(featureSet);
  }
}
