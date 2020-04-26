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
import javax.persistence.EmbeddedId;
import lombok.Getter;
import lombok.Setter;

/** Feast entity object. Contains name, type as well as domain metadata about the entity. */
@Getter
@Setter
@javax.persistence.Entity
public class Entity {
  @EmbeddedId private EntityReference reference;

  private String type;

  public Entity() {}

  private Entity(String name, ValueType.Enum type) {
    this.setReference(new EntityReference(name));
    this.setType(type.toString());
  }

  public static Entity withRef(EntityReference entityRef) {
    Entity entity = new Entity();
    entity.setReference(entityRef);
    return entity;
  }

  public static Entity fromProto(EntitySpec entitySpec) {
    Entity entity = new Entity(entitySpec.getName(), entitySpec.getValueType());
    return entity;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Entity feature = (Entity) o;
    return getReference().equals(feature.getReference()) && getType().equals(feature.getType());
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), getReference(), getType());
  }
}
