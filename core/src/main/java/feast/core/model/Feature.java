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
import java.util.List;
import java.util.Objects;
import javax.persistence.*;
import javax.persistence.Entity;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
public class Feature extends Field {

  @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
  @JoinColumns({
    @JoinColumn(name = "project", referencedColumnName = "project"),
    @JoinColumn(name = "feature_set", referencedColumnName = "feature_set"),
    @JoinColumn(name = "version", referencedColumnName = "version"),
    @JoinColumn(name = "name", referencedColumnName = "name")
  })
  private List<Statistics> statistics;

  public Feature() {}

  public Feature(String name, ValueType.Enum type) {
    this.setId(new FieldId());
    this.setName(name);
    this.setType(type.toString());
  }

  public Feature(FeatureSpec featureSpec) {
    this.setId(new FieldId());
    this.setName(featureSpec.getName());
    this.setType(featureSpec.getValueType().toString());

    switch (featureSpec.getPresenceConstraintsCase()) {
      case PRESENCE:
        this.setPresence(featureSpec.getPresence().toByteArray());
        break;
      case GROUP_PRESENCE:
        this.setGroupPresence(featureSpec.getGroupPresence().toByteArray());
        break;
      case PRESENCECONSTRAINTS_NOT_SET:
        break;
    }

    switch (featureSpec.getShapeTypeCase()) {
      case SHAPE:
        this.setShape(featureSpec.getShape().toByteArray());
        break;
      case VALUE_COUNT:
        this.setValueCount(featureSpec.getValueCount().toByteArray());
        break;
      case SHAPETYPE_NOT_SET:
        break;
    }

    switch (featureSpec.getDomainInfoCase()) {
      case DOMAIN:
        this.setDomain(featureSpec.getDomain());
        break;
      case INT_DOMAIN:
        this.setIntDomain(featureSpec.getIntDomain().toByteArray());
        break;
      case FLOAT_DOMAIN:
        this.setFloatDomain(featureSpec.getFloatDomain().toByteArray());
        break;
      case STRING_DOMAIN:
        this.setStringDomain(featureSpec.getStringDomain().toByteArray());
        break;
      case BOOL_DOMAIN:
        this.setBoolDomain(featureSpec.getBoolDomain().toByteArray());
        break;
      case STRUCT_DOMAIN:
        this.setStructDomain(featureSpec.getStructDomain().toByteArray());
        break;
      case NATURAL_LANGUAGE_DOMAIN:
        this.setNaturalLanguageDomain(featureSpec.getNaturalLanguageDomain().toByteArray());
        break;
      case IMAGE_DOMAIN:
        this.setImageDomain(featureSpec.getImageDomain().toByteArray());
        break;
      case MID_DOMAIN:
        this.setMidDomain(featureSpec.getMidDomain().toByteArray());
        break;
      case URL_DOMAIN:
        this.setUrlDomain(featureSpec.getUrlDomain().toByteArray());
        break;
      case TIME_DOMAIN:
        this.setTimeDomain(featureSpec.getTimeDomain().toByteArray());
        break;
      case TIME_OF_DAY_DOMAIN:
        this.setTimeOfDayDomain(featureSpec.getTimeOfDayDomain().toByteArray());
        break;
      case DOMAININFO_NOT_SET:
        break;
    }
  }

  public void addStatistics(Statistics newStatistic) {
    this.statistics.add(newStatistic);
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
    return getId().equals(feature.getId()) && getType().equals(feature.getType());
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), getId(), getType());
  }
}
