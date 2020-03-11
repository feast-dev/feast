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
import feast.core.FeatureSetProto.FeatureSpec;
import feast.core.util.TypeConversion;
import feast.types.ValueProto.ValueType;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import javax.persistence.Column;
import javax.persistence.Embeddable;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Embeddable
public class Field {

  // Name of the feature
  @Column(name = "name", nullable = false)
  private String name;

  // Type of the feature, should correspond with feast.types.ValueType
  @Column(name = "type", nullable = false)
  private String type;

  // Version of the field
  @Column(name = "version")
  private int version;

  // Project that this field belongs to
  @Column(name = "project")
  private String project;

  // Labels that this field belongs to
  @Column(name = "labels", columnDefinition = "text")
  private String labels;

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

  public Field() {}

  public Field(String name, ValueType.Enum type) {
    this.name = name;
    this.type = type.toString();
  }

  public Field(FeatureSpec featureSpec) {
    this.name = featureSpec.getName();
    this.type = featureSpec.getValueType().toString();
    this.labels = TypeConversion.convertMapToJsonString(featureSpec.getLabelsMap());

    switch (featureSpec.getPresenceConstraintsCase()) {
      case PRESENCE:
        this.presence = featureSpec.getPresence().toByteArray();
        break;
      case GROUP_PRESENCE:
        this.groupPresence = featureSpec.getGroupPresence().toByteArray();
        break;
      case PRESENCECONSTRAINTS_NOT_SET:
        break;
    }

    switch (featureSpec.getShapeTypeCase()) {
      case SHAPE:
        this.shape = featureSpec.getShape().toByteArray();
        break;
      case VALUE_COUNT:
        this.valueCount = featureSpec.getValueCount().toByteArray();
        break;
      case SHAPETYPE_NOT_SET:
        break;
    }

    switch (featureSpec.getDomainInfoCase()) {
      case DOMAIN:
        this.domain = featureSpec.getDomain();
        break;
      case INT_DOMAIN:
        this.intDomain = featureSpec.getIntDomain().toByteArray();
        break;
      case FLOAT_DOMAIN:
        this.floatDomain = featureSpec.getFloatDomain().toByteArray();
        break;
      case STRING_DOMAIN:
        this.stringDomain = featureSpec.getStringDomain().toByteArray();
        break;
      case BOOL_DOMAIN:
        this.boolDomain = featureSpec.getBoolDomain().toByteArray();
        break;
      case STRUCT_DOMAIN:
        this.structDomain = featureSpec.getStructDomain().toByteArray();
        break;
      case NATURAL_LANGUAGE_DOMAIN:
        this.naturalLanguageDomain = featureSpec.getNaturalLanguageDomain().toByteArray();
        break;
      case IMAGE_DOMAIN:
        this.imageDomain = featureSpec.getImageDomain().toByteArray();
        break;
      case MID_DOMAIN:
        this.midDomain = featureSpec.getMidDomain().toByteArray();
        break;
      case URL_DOMAIN:
        this.urlDomain = featureSpec.getUrlDomain().toByteArray();
        break;
      case TIME_DOMAIN:
        this.timeDomain = featureSpec.getTimeDomain().toByteArray();
        break;
      case TIME_OF_DAY_DOMAIN:
        this.timeOfDayDomain = featureSpec.getTimeOfDayDomain().toByteArray();
        break;
      case DOMAININFO_NOT_SET:
        break;
    }
  }

  public Field(EntitySpec entitySpec) {
    this.name = entitySpec.getName();
    this.type = entitySpec.getValueType().toString();

    switch (entitySpec.getPresenceConstraintsCase()) {
      case PRESENCE:
        this.presence = entitySpec.getPresence().toByteArray();
        break;
      case GROUP_PRESENCE:
        this.groupPresence = entitySpec.getGroupPresence().toByteArray();
        break;
      case PRESENCECONSTRAINTS_NOT_SET:
        break;
    }

    switch (entitySpec.getShapeTypeCase()) {
      case SHAPE:
        this.shape = entitySpec.getShape().toByteArray();
        break;
      case VALUE_COUNT:
        this.valueCount = entitySpec.getValueCount().toByteArray();
        break;
      case SHAPETYPE_NOT_SET:
        break;
    }

    switch (entitySpec.getDomainInfoCase()) {
      case DOMAIN:
        this.domain = entitySpec.getDomain();
        break;
      case INT_DOMAIN:
        this.intDomain = entitySpec.getIntDomain().toByteArray();
        break;
      case FLOAT_DOMAIN:
        this.floatDomain = entitySpec.getFloatDomain().toByteArray();
        break;
      case STRING_DOMAIN:
        this.stringDomain = entitySpec.getStringDomain().toByteArray();
        break;
      case BOOL_DOMAIN:
        this.boolDomain = entitySpec.getBoolDomain().toByteArray();
        break;
      case STRUCT_DOMAIN:
        this.structDomain = entitySpec.getStructDomain().toByteArray();
        break;
      case NATURAL_LANGUAGE_DOMAIN:
        this.naturalLanguageDomain = entitySpec.getNaturalLanguageDomain().toByteArray();
        break;
      case IMAGE_DOMAIN:
        this.imageDomain = entitySpec.getImageDomain().toByteArray();
        break;
      case MID_DOMAIN:
        this.midDomain = entitySpec.getMidDomain().toByteArray();
        break;
      case URL_DOMAIN:
        this.urlDomain = entitySpec.getUrlDomain().toByteArray();
        break;
      case TIME_DOMAIN:
        this.timeDomain = entitySpec.getTimeDomain().toByteArray();
        break;
      case TIME_OF_DAY_DOMAIN:
        this.timeOfDayDomain = entitySpec.getTimeOfDayDomain().toByteArray();
        break;
      case DOMAININFO_NOT_SET:
        break;
    }
  }

  public Map<String, String> getLabelsJSON() {
    return TypeConversion.convertJsonStringToMap(this.labels);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Field field = (Field) o;
    return Objects.equals(name, field.name)
        && Objects.equals(type, field.type)
        && Objects.equals(project, field.project)
        && Arrays.equals(presence, field.presence)
        && Arrays.equals(groupPresence, field.groupPresence)
        && Arrays.equals(shape, field.shape)
        && Arrays.equals(valueCount, field.valueCount)
        && Objects.equals(domain, field.domain)
        && Arrays.equals(intDomain, field.intDomain)
        && Arrays.equals(floatDomain, field.floatDomain)
        && Arrays.equals(stringDomain, field.stringDomain)
        && Arrays.equals(boolDomain, field.boolDomain)
        && Arrays.equals(structDomain, field.structDomain)
        && Arrays.equals(naturalLanguageDomain, field.naturalLanguageDomain)
        && Arrays.equals(imageDomain, field.imageDomain)
        && Arrays.equals(midDomain, field.midDomain)
        && Arrays.equals(urlDomain, field.urlDomain)
        && Arrays.equals(timeDomain, field.timeDomain)
        && Arrays.equals(timeOfDayDomain, field.timeOfDayDomain);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), name, type);
  }
}
