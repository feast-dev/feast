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

import java.io.Serializable;
import java.util.Objects;
import javax.persistence.Column;
import javax.persistence.Embeddable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Embeddable
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class FieldId implements Serializable {
  // Project the field belongs to
  @Column(nullable = false)
  private String project;

  // Feature set the field belongs to
  @Column(name = "feature_set", nullable = false)
  private String featureSet;

  // Version of the feature set this field belongs to
  @Column(nullable = false)
  private int version;

  // Name of the field
  @Column(nullable = false)
  private String name;

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FieldId fieldId = (FieldId) o;
    return Objects.equals(name, fieldId.getName())
        && Objects.equals(project, fieldId.getProject())
        && Objects.equals(featureSet, fieldId.getFeatureSet());
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), project, featureSet, name);
  }
}
