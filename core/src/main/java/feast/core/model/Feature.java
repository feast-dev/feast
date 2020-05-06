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

import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.core.FeatureSetProto.FeatureSpec.Builder;
import feast.core.util.TypeConversion;
import feast.types.ValueProto.ValueType;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import javax.persistence.*;
import javax.persistence.Entity;
import lombok.Getter;
import lombok.Setter;
import org.tensorflow.metadata.v0.*;

/**
 * Feature belonging to a featureset. Contains name, type as well as domain metadata about the
 * feature.
 */
@Getter
@Setter
@Entity
@Table(
    name = "features",
    uniqueConstraints = @UniqueConstraint(columnNames = {"name", "feature_set_id"}))
public class Feature {

  @Id @GeneratedValue private Long id;

  private String name;

  @ManyToOne(fetch = FetchType.LAZY)
  private FeatureSet featureSet;

  /** Data type of the feature. String representation of {@link ValueType} * */
  private String type;

  // Labels for this feature
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

  public Feature() {}
  // Whether this feature has been archived. A archived feature cannot be
  // retrieved from or written to.
  private boolean archived = false;

  private Feature(String name, ValueType.Enum type) {
    this.setName(name);
    this.setType(type.toString());
  }

  public static Feature fromProto(FeatureSpec featureSpec) {
    Feature feature = new Feature(featureSpec.getName(), featureSpec.getValueType());
    feature.labels = TypeConversion.convertMapToJsonString(featureSpec.getLabelsMap());
    feature.updateSchema(featureSpec);
    return feature;
  }

  public FeatureSpec toProto() throws InvalidProtocolBufferException {
    Builder featureSpecBuilder =
        FeatureSpec.newBuilder().setName(getName()).setValueType(ValueType.Enum.valueOf(getType()));

    if (getPresence() != null) {
      featureSpecBuilder.setPresence(FeaturePresence.parseFrom(getPresence()));
    } else if (getGroupPresence() != null) {
      featureSpecBuilder.setGroupPresence(FeaturePresenceWithinGroup.parseFrom(getGroupPresence()));
    }

    if (getShape() != null) {
      featureSpecBuilder.setShape(FixedShape.parseFrom(getShape()));
    } else if (getValueCount() != null) {
      featureSpecBuilder.setValueCount(ValueCount.parseFrom(getValueCount()));
    }

    if (getDomain() != null) {
      featureSpecBuilder.setDomain(getDomain());
    } else if (getIntDomain() != null) {
      featureSpecBuilder.setIntDomain(IntDomain.parseFrom(getIntDomain()));
    } else if (getFloatDomain() != null) {
      featureSpecBuilder.setFloatDomain(FloatDomain.parseFrom(getFloatDomain()));
    } else if (getStringDomain() != null) {
      featureSpecBuilder.setStringDomain(StringDomain.parseFrom(getStringDomain()));
    } else if (getBoolDomain() != null) {
      featureSpecBuilder.setBoolDomain(BoolDomain.parseFrom(getBoolDomain()));
    } else if (getStructDomain() != null) {
      featureSpecBuilder.setStructDomain(StructDomain.parseFrom(getStructDomain()));
    } else if (getNaturalLanguageDomain() != null) {
      featureSpecBuilder.setNaturalLanguageDomain(
          NaturalLanguageDomain.parseFrom(getNaturalLanguageDomain()));
    } else if (getImageDomain() != null) {
      featureSpecBuilder.setImageDomain(ImageDomain.parseFrom(getImageDomain()));
    } else if (getMidDomain() != null) {
      featureSpecBuilder.setMidDomain(MIDDomain.parseFrom(getMidDomain()));
    } else if (getUrlDomain() != null) {
      featureSpecBuilder.setUrlDomain(URLDomain.parseFrom(getUrlDomain()));
    } else if (getTimeDomain() != null) {
      featureSpecBuilder.setTimeDomain(TimeDomain.parseFrom(getTimeDomain()));
    } else if (getTimeOfDayDomain() != null) {
      featureSpecBuilder.setTimeOfDayDomain(TimeOfDayDomain.parseFrom(getTimeOfDayDomain()));
    }

    if (getLabels() != null) {
      featureSpecBuilder.putAllLabels(getLabels());
    }
    return featureSpecBuilder.build();
  }

  private void updateSchema(FeatureSpec featureSpec) {
    switch (featureSpec.getPresenceConstraintsCase()) {
      case PRESENCE:
        setPresence(featureSpec.getPresence().toByteArray());
        break;
      case GROUP_PRESENCE:
        setGroupPresence(featureSpec.getGroupPresence().toByteArray());
        break;
      case PRESENCECONSTRAINTS_NOT_SET:
        break;
    }

    switch (featureSpec.getShapeTypeCase()) {
      case SHAPE:
        setShape(featureSpec.getShape().toByteArray());
        break;
      case VALUE_COUNT:
        setValueCount(featureSpec.getValueCount().toByteArray());
        break;
      case SHAPETYPE_NOT_SET:
        break;
    }

    switch (featureSpec.getDomainInfoCase()) {
      case DOMAIN:
        setDomain(featureSpec.getDomain());
        break;
      case INT_DOMAIN:
        setIntDomain(featureSpec.getIntDomain().toByteArray());
        break;
      case FLOAT_DOMAIN:
        setFloatDomain(featureSpec.getFloatDomain().toByteArray());
        break;
      case STRING_DOMAIN:
        setStringDomain(featureSpec.getStringDomain().toByteArray());
        break;
      case BOOL_DOMAIN:
        setBoolDomain(featureSpec.getBoolDomain().toByteArray());
        break;
      case STRUCT_DOMAIN:
        setStructDomain(featureSpec.getStructDomain().toByteArray());
        break;
      case NATURAL_LANGUAGE_DOMAIN:
        setNaturalLanguageDomain(featureSpec.getNaturalLanguageDomain().toByteArray());
        break;
      case IMAGE_DOMAIN:
        setImageDomain(featureSpec.getImageDomain().toByteArray());
        break;
      case MID_DOMAIN:
        setMidDomain(featureSpec.getMidDomain().toByteArray());
        break;
      case URL_DOMAIN:
        setUrlDomain(featureSpec.getUrlDomain().toByteArray());
        break;
      case TIME_DOMAIN:
        setTimeDomain(featureSpec.getTimeDomain().toByteArray());
        break;
      case TIME_OF_DAY_DOMAIN:
        setTimeOfDayDomain(featureSpec.getTimeOfDayDomain().toByteArray());
        break;
      case DOMAININFO_NOT_SET:
        break;
    }
  }

  /** Archive this feature. */
  public void archive() {
    this.archived = true;
  }

  /**
   * Update the feature object with a valid feature spec. Only schema changes are allowed.
   *
   * @param featureSpec {@link FeatureSpec} containing schema changes.
   */
  public void updateFromProto(FeatureSpec featureSpec) {
    if (isArchived()) {
      throw new IllegalArgumentException(
          String.format(
              "You are attempting to create a feature %s that was previously archived. This isn't allowed. Please create a new feature with a different name.",
              featureSpec.getName()));
    }
    if (ValueType.Enum.valueOf(type) != featureSpec.getValueType()) {
      throw new IllegalArgumentException(
          String.format(
              "You are attempting to change the type of feature %s from %s to %s. This isn't allowed. Please create a new feature.",
              featureSpec.getName(), type, featureSpec.getValueType()));
    }
    updateSchema(featureSpec);
  }

  public Map<String, String> getLabels() {
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
    Feature feature = (Feature) o;
    return getName().equals(feature.getName())
        && getType().equals(feature.getType())
        && isArchived() == (feature.isArchived())
        && Objects.equals(getLabels(), feature.getLabels())
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
    return Objects.hash(super.hashCode(), getName(), getType(), getLabels());
  }
}
