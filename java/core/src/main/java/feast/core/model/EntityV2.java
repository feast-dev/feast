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

import com.google.protobuf.Timestamp;
import feast.core.util.TypeConversion;
import feast.proto.core.EntityProto;
import feast.proto.core.EntityProto.*;
import feast.proto.types.ValueProto.ValueType;
import java.util.Map;
import javax.persistence.*;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@javax.persistence.Entity
@Table(
    name = "entities_v2",
    uniqueConstraints = @UniqueConstraint(columnNames = {"name", "project_name"}))
public class EntityV2 extends AbstractTimestampEntity {
  @Id @GeneratedValue private long id;

  // Name of the Entity
  @Column(name = "name", nullable = false)
  private String name;

  // Project that this Entity belongs to
  @ManyToOne(fetch = FetchType.LAZY)
  @JoinColumn(name = "project_name")
  private Project project;

  // Description of entity
  @Column(name = "description", columnDefinition = "text")
  private String description;

  // Columns of entities
  /** Data type of each entity column: String representation of {@link ValueType} * */
  private String type;

  // User defined metadata
  @Column(name = "labels", columnDefinition = "text")
  private String labels;

  public EntityV2() {
    super();
  }

  /**
   * EntityV2 object supports Entity registration in FeatureTable.
   *
   * <p>This data model supports Scalar Entity and would allow ease of discovery of entities and
   * reasoning when used in association with FeatureTable.
   *
   * @param name name
   * @param description description
   * @param type type
   * @param labels labels
   */
  public EntityV2(
      String name, String description, ValueType.Enum type, Map<String, String> labels) {
    this.name = name;
    this.description = description;
    this.type = type.toString();
    this.labels = TypeConversion.convertMapToJsonString(labels);
  }

  public static EntityV2 fromProto(EntityProto.Entity entityProto) {
    EntitySpecV2 spec = entityProto.getSpec();

    return new EntityV2(
        spec.getName(), spec.getDescription(), spec.getValueType(), spec.getLabelsMap());
  }

  public EntityProto.Entity toProto() {
    EntityMeta.Builder meta =
        EntityMeta.newBuilder()
            .setCreatedTimestamp(
                Timestamp.newBuilder().setSeconds(super.getCreated().getTime() / 1000L))
            .setLastUpdatedTimestamp(
                Timestamp.newBuilder().setSeconds(super.getLastUpdated().getTime() / 1000L));

    EntitySpecV2.Builder spec =
        EntitySpecV2.newBuilder()
            .setName(getName())
            .setDescription(getDescription())
            .setValueType(ValueType.Enum.valueOf(getType()))
            .putAllLabels(TypeConversion.convertJsonStringToMap(labels));

    // Build Entity
    EntityProto.Entity entity = EntityProto.Entity.newBuilder().setMeta(meta).setSpec(spec).build();
    return entity;
  }

  /**
   * Updates the existing entity from a proto.
   *
   * @param entityProto EntityProto with updated spec
   * @param projectName Project namespace of Entity which is to be created/updated
   */
  public void updateFromProto(EntityProto.Entity entityProto, String projectName) {
    EntitySpecV2 spec = entityProto.getSpec();

    // Validate no change to type
    if (!spec.getValueType().equals(ValueType.Enum.valueOf(getType()))) {
      throw new IllegalArgumentException(
          String.format(
              "You are attempting to change the type of this entity in %s project from %s to %s. This isn't allowed. Please create a new entity.",
              projectName, getType(), spec.getValueType().toString()));
    }

    // 2. Update description, labels
    this.setDescription(spec.getDescription());
    this.setLabels(TypeConversion.convertMapToJsonString(spec.getLabelsMap()));
  }

  /**
   * Determine whether an entity has all the specified labels.
   *
   * @param labelsFilter labels contain key-value mapping for labels attached to the Entity
   * @return boolean True if Entity contains all labels in the labelsFilter
   */
  public boolean hasAllLabels(Map<String, String> labelsFilter) {
    Map<String, String> LabelsMap = this.getLabelsMap();
    for (String key : labelsFilter.keySet()) {
      if (!LabelsMap.containsKey(key) || !LabelsMap.get(key).equals(labelsFilter.get(key))) {
        return false;
      }
    }
    return true;
  }

  public Map<String, String> getLabelsMap() {
    return TypeConversion.convertJsonStringToMap(this.getLabels());
  }
}
