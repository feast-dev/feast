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
import lombok.Getter;
import lombok.Setter;

/** Feast entity object. Contains name, type as well as domain metadata about the entity. */
@Getter
@Setter
@javax.persistence.Entity
public class Entity extends Field {

  public Entity() {}

  public Entity(FieldId fieldId) {
    this.setId(fieldId);
  }

  public Entity(String name, ValueType.Enum type) {
    this.setId(new FieldId());
    this.setName(name);
    this.setType(type.toString());
  }

  public static Entity fromProto(EntitySpec entitySpec) {
    Entity entity = new Entity(entitySpec.getName(), entitySpec.getValueType());

    switch (entitySpec.getPresenceConstraintsCase()) {
      case PRESENCE:
        entity.setPresence(entitySpec.getPresence().toByteArray());
        break;
      case GROUP_PRESENCE:
        entity.setGroupPresence(entitySpec.getGroupPresence().toByteArray());
        break;
      case PRESENCECONSTRAINTS_NOT_SET:
        break;
    }

    switch (entitySpec.getShapeTypeCase()) {
      case SHAPE:
        entity.setShape(entitySpec.getShape().toByteArray());
        break;
      case VALUE_COUNT:
        entity.setValueCount(entitySpec.getValueCount().toByteArray());
        break;
      case SHAPETYPE_NOT_SET:
        break;
    }

    switch (entitySpec.getDomainInfoCase()) {
      case DOMAIN:
        entity.setDomain(entitySpec.getDomain());
        break;
      case INT_DOMAIN:
        entity.setIntDomain(entitySpec.getIntDomain().toByteArray());
        break;
      case FLOAT_DOMAIN:
        entity.setFloatDomain(entitySpec.getFloatDomain().toByteArray());
        break;
      case STRING_DOMAIN:
        entity.setStringDomain(entitySpec.getStringDomain().toByteArray());
        break;
      case BOOL_DOMAIN:
        entity.setBoolDomain(entitySpec.getBoolDomain().toByteArray());
        break;
      case STRUCT_DOMAIN:
        entity.setStructDomain(entitySpec.getStructDomain().toByteArray());
        break;
      case NATURAL_LANGUAGE_DOMAIN:
        entity.setNaturalLanguageDomain(entitySpec.getNaturalLanguageDomain().toByteArray());
        break;
      case IMAGE_DOMAIN:
        entity.setImageDomain(entitySpec.getImageDomain().toByteArray());
        break;
      case MID_DOMAIN:
        entity.setMidDomain(entitySpec.getMidDomain().toByteArray());
        break;
      case URL_DOMAIN:
        entity.setUrlDomain(entitySpec.getUrlDomain().toByteArray());
        break;
      case TIME_DOMAIN:
        entity.setTimeDomain(entitySpec.getTimeDomain().toByteArray());
        break;
      case TIME_OF_DAY_DOMAIN:
        entity.setTimeOfDayDomain(entitySpec.getTimeOfDayDomain().toByteArray());
        break;
      case DOMAININFO_NOT_SET:
        break;
    }
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
    return getId().equals(feature.getId()) && getType().equals(feature.getType());
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), getId(), getType());
  }
}
