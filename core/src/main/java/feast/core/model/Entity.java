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

import feast.core.FeatureSetProto.EntitySpec;
import feast.types.ValueProto.ValueType;
import java.util.Objects;
import javax.persistence.*;
import lombok.Getter;
import lombok.Setter;

/** Feast entity object. Contains name and type of the entity. */
@Getter
@Setter
@javax.persistence.Entity
@Table(
    name = "entities",
    uniqueConstraints = @UniqueConstraint(columnNames = {"name", "feature_set_id"}))
public class Entity {

  @Id @GeneratedValue private Long id;

  private String name;

  @ManyToOne(fetch = FetchType.LAZY)
  private FeatureSet featureSet;

  /** Data type of the entity. String representation of {@link ValueType} * */
  private String type;

  public Entity() {}

  private Entity(String name, ValueType.Enum type) {
    this.setName(name);
    this.setType(type.toString());
  }

  public static Entity fromProto(EntitySpec entitySpec) {
    Entity entity = new Entity(entitySpec.getName(), entitySpec.getValueType());
    return entity;
  }

  public EntitySpec toProto() {
    return EntitySpec.newBuilder().setName(name).setValueType(ValueType.Enum.valueOf(type)).build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Entity entity = (Entity) o;
    return getName().equals(entity.getName()) && getType().equals(entity.getType());
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), getName(), getType());
  }
}
