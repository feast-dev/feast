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

import feast.core.FeatureSetProto.FeatureSpec;
import feast.types.ValueProto.ValueType;
import java.util.Arrays;
import java.util.Objects;
import javax.persistence.*;
import javax.persistence.Entity;
import lombok.Getter;
import lombok.Setter;

/**
 * Feature belonging to a featureset. Contains name, type as well as domain metadata about the
 * feature.
 */
@Getter
@Setter
@Entity
public class Feature extends Field {

  public Feature() {}

  public Feature(FieldId fieldId) {
    this.setId(fieldId);
  }

  public Feature(String name, ValueType.Enum type) {
    this.setId(new FieldId());
    this.setName(name);
    this.setType(type.toString());
  }

  public static Feature fromProto(FeatureSpec featureSpec) {
    Feature feature = new Feature(featureSpec.getName(), featureSpec.getValueType());

    switch (featureSpec.getPresenceConstraintsCase()) {
      case PRESENCE:
        feature.setPresence(featureSpec.getPresence().toByteArray());
        break;
      case GROUP_PRESENCE:
        feature.setGroupPresence(featureSpec.getGroupPresence().toByteArray());
        break;
      case PRESENCECONSTRAINTS_NOT_SET:
        break;
    }

    switch (featureSpec.getShapeTypeCase()) {
      case SHAPE:
        feature.setShape(featureSpec.getShape().toByteArray());
        break;
      case VALUE_COUNT:
        feature.setValueCount(featureSpec.getValueCount().toByteArray());
        break;
      case SHAPETYPE_NOT_SET:
        break;
    }

    switch (featureSpec.getDomainInfoCase()) {
      case DOMAIN:
        feature.setDomain(featureSpec.getDomain());
        break;
      case INT_DOMAIN:
        feature.setIntDomain(featureSpec.getIntDomain().toByteArray());
        break;
      case FLOAT_DOMAIN:
        feature.setFloatDomain(featureSpec.getFloatDomain().toByteArray());
        break;
      case STRING_DOMAIN:
        feature.setStringDomain(featureSpec.getStringDomain().toByteArray());
        break;
      case BOOL_DOMAIN:
        feature.setBoolDomain(featureSpec.getBoolDomain().toByteArray());
        break;
      case STRUCT_DOMAIN:
        feature.setStructDomain(featureSpec.getStructDomain().toByteArray());
        break;
      case NATURAL_LANGUAGE_DOMAIN:
        feature.setNaturalLanguageDomain(featureSpec.getNaturalLanguageDomain().toByteArray());
        break;
      case IMAGE_DOMAIN:
        feature.setImageDomain(featureSpec.getImageDomain().toByteArray());
        break;
      case MID_DOMAIN:
        feature.setMidDomain(featureSpec.getMidDomain().toByteArray());
        break;
      case URL_DOMAIN:
        feature.setUrlDomain(featureSpec.getUrlDomain().toByteArray());
        break;
      case TIME_DOMAIN:
        feature.setTimeDomain(featureSpec.getTimeDomain().toByteArray());
        break;
      case TIME_OF_DAY_DOMAIN:
        feature.setTimeOfDayDomain(featureSpec.getTimeOfDayDomain().toByteArray());
        break;
      case DOMAININFO_NOT_SET:
        break;
    }
    return feature;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Feature feature = (Feature) o;
    return Objects.equals(getId(), feature.getId())
        && Arrays.equals(getPresence(), feature.getPresence())
        && Arrays.equals(getGroupPresence(), feature.getGroupPresence())
        && Arrays.equals(getShape(), feature.getShape())
        && Arrays.equals(getValueCount(), feature.getValueCount())
        && Objects.equals(getDomain(), feature.getDomain())
        && Arrays.equals(getIntDomain(), feature.getIntDomain())
        && Arrays.equals(getFloatDomain(), feature.getFloatDomain())
        && Arrays.equals(getStringDomain(), feature.getStringDomain())
        && Arrays.equals(getBoolDomain(), feature.getBoolDomain())
        && Arrays.equals(getStructDomain(), feature.getStructDomain())
        && Arrays.equals(getNaturalLanguageDomain(), feature.getNaturalLanguageDomain())
        && Arrays.equals(getImageDomain(), feature.getImageDomain())
        && Arrays.equals(getMidDomain(), feature.getMidDomain())
        && Arrays.equals(getUrlDomain(), feature.getUrlDomain())
        && Arrays.equals(getTimeDomain(), feature.getTimeDomain())
        && Arrays.equals(getTimeDomain(), feature.getTimeOfDayDomain());
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), getId(), getType());
  }
}
