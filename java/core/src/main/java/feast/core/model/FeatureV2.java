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
package feast.core.model;

import feast.core.util.TypeConversion;
import feast.proto.core.FeatureProto.FeatureSpecV2;
import feast.proto.types.ValueProto.ValueType;
import java.util.Map;
import java.util.Objects;
import javax.persistence.*;
import javax.persistence.Entity;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

/** Defines a single Feature defined in a {@link FeatureTable} */
@Getter
@Entity
@Setter(AccessLevel.PRIVATE)
@Table(
    name = "features_v2",
    uniqueConstraints = @UniqueConstraint(columnNames = {"name", "feature_table_id"}))
public class FeatureV2 {
  @Id @GeneratedValue private long id;

  // Name of the Feature
  private String name;

  // Feature Table where this Feature is defined in.
  @ManyToOne(fetch = FetchType.LAZY)
  @JoinColumn(name = "feature_table_id", nullable = false)
  private FeatureTable featureTable;

  // Value type of the feature. String representation of ValueType.
  @Enumerated(EnumType.STRING)
  @Column(name = "type")
  private ValueType.Enum type;

  // User defined metadata labels for this feature encoded a JSON string.
  @Column(name = "labels", columnDefinition = "text")
  private String labelsJSON;

  public FeatureV2() {};

  public FeatureV2(FeatureTable table, String name, ValueType.Enum type, String labelsJSON) {
    this.featureTable = table;
    this.name = name;
    this.type = type;
    this.labelsJSON = labelsJSON;
  }

  /**
   * Construct Feature from Protobuf spec representation.
   *
   * @param table the FeatureTable to associate the constructed feature with.
   * @param spec the Protobuf spec to contruct the Feature from.
   * @return constructed Feature from the given Protobuf spec.
   */
  public static FeatureV2 fromProto(FeatureTable table, FeatureSpecV2 spec) {
    String labelsJSON = TypeConversion.convertMapToJsonString(spec.getLabelsMap());
    return new FeatureV2(table, spec.getName(), spec.getValueType(), labelsJSON);
  }

  /**
   * Convert this Feature to its Protobuf representation.
   *
   * @return protobuf representation
   */
  public FeatureSpecV2 toProto() {
    Map<String, String> labels = TypeConversion.convertJsonStringToMap(getLabelsJSON());
    return FeatureSpecV2.newBuilder()
        .setName(getName())
        .setValueType(getType())
        .putAllLabels(labels)
        .build();
  }

  /**
   * Update the Feature from the given Protobuf representation.
   *
   * @param spec the Protobuf spec to update the Feature from.
   * @throws IllegalArgumentException if the update will make prohibited changes.
   */
  public void updateFromProto(FeatureSpecV2 spec) {
    // Check for prohibited changes made in spec
    if (!getName().equals(spec.getName())) {
      throw new IllegalArgumentException(
          String.format(
              "Updating the name of a registered Feature is not allowed: %s to %s",
              getName(), spec.getName()));
    }
    // Update feature type
    this.setType(spec.getValueType());

    // Update Feature based on spec
    this.labelsJSON = TypeConversion.convertMapToJsonString(spec.getLabelsMap());
  }

  /**
   * Return a boolean to indicate if Feature contains all specified labels.
   *
   * @param labelsFilter contain labels that should be attached to Feature
   * @return boolean True if Feature contains all labels in the labelsFilter
   */
  public boolean hasAllLabels(Map<String, String> labelsFilter) {
    Map<String, String> featureLabelsMap = TypeConversion.convertJsonStringToMap(getLabelsJSON());
    for (String key : labelsFilter.keySet()) {
      if (!featureLabelsMap.containsKey(key)
          || !featureLabelsMap.get(key).equals(labelsFilter.get(key))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getName(), getType(), getLabelsJSON());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    FeatureV2 feature = (FeatureV2) o;
    return getName().equals(feature.getName())
        && getType().equals(feature.getType())
        && getLabelsJSON().equals(feature.getLabelsJSON());
  }
}
