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

import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.types.ValueProto.ValueType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;

@Getter
@Setter
@Entity
@Table(name = "feature_sets")
public class FeatureSet extends AbstractTimestampEntity implements Comparable<FeatureSet> {

  // Id of the featureSet, defined as name:version
  @Id
  @Column(name = "id", nullable = false, unique = true)
  private String id;

  // Name of the featureSet
  @Column(name = "name", nullable = false)
  private String name;

  // Version of the featureSet
  @Column(name = "version")
  private int version;

  // Max allowed staleness for features in this featureSet.
  @Column(name = "max_age")
  private long maxAgeSeconds;

  @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.EAGER)
  @Fetch(value = FetchMode.SUBSELECT)
  @JoinColumn(name = "entities")
  private List<Field> entities;

  // Features inside this featureSet
  @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.EAGER)
  @Fetch(value = FetchMode.SUBSELECT)
  @JoinColumn(name = "features")
  private List<Field> features;

  // Source on which feature rows can be found
  @ManyToOne(cascade = CascadeType.ALL, fetch = FetchType.EAGER)
  @JoinColumn(name = "source")
  private Source source;

  public FeatureSet() {
    super();
  }

  public FeatureSet(
      String name,
      int version,
      long maxAgeSeconds,
      List<Field> entities,
      List<Field> features,
      Source source) {
    this.id = String.format("%s:%s", name, version);
    this.name = name;
    this.version = version;
    this.maxAgeSeconds = maxAgeSeconds;
    this.entities = entities;
    this.features = features;
    this.source = source;
  }

  public static FeatureSet fromProto(FeatureSetSpec featureSetSpec) {
    Source source = Source.fromProto(featureSetSpec.getSource());
    String id = String.format("%s:%d", featureSetSpec.getName(), featureSetSpec.getVersion());
    List<Field> features = new ArrayList<>();
    for (FeatureSpec feature : featureSetSpec.getFeaturesList()) {
      features.add(new Field(id, feature.getName(), feature.getValueType()));
    }
    List<Field> entities = new ArrayList<>();
    for (EntitySpec entity : featureSetSpec.getEntitiesList()) {
      entities.add(new Field(id, entity.getName(), entity.getValueType()));
    }

    return new FeatureSet(
        featureSetSpec.getName(),
        featureSetSpec.getVersion(),
        featureSetSpec.getMaxAge().getSeconds(),
        entities,
        features,
        source);
  }

  public FeatureSetSpec toProto() throws InvalidProtocolBufferException {
    List<EntitySpec> entitySpecs = new ArrayList<>();
    for (Field entity : entities) {
      entitySpecs.add(
          EntitySpec.newBuilder()
              .setName(entity.getName())
              .setValueType(ValueType.Enum.valueOf(entity.getType()))
              .build());
    }

    List<FeatureSpec> featureSpecs = new ArrayList<>();
    for (Field feature : features) {
      featureSpecs.add(
          FeatureSpec.newBuilder()
              .setName(feature.getName())
              .setValueType(ValueType.Enum.valueOf(feature.getType()))
              .build());
    }
    return FeatureSetSpec.newBuilder()
        .setName(name)
        .setVersion(version)
        .setMaxAge(Duration.newBuilder().setSeconds(maxAgeSeconds))
        .addAllEntities(entitySpecs)
        .addAllFeatures(featureSpecs)
        .setSource(source.toProto())
        .build();
  }

  /**
   * Checks if the given featureSet's schema and source has is different from this one.
   *
   * @param other FeatureSet to compare to
   * @return boolean denoting if the source or schema have changed.
   */
  public boolean equalTo(FeatureSet other) {
    if(!name.equals(other.getName())){
      return false;
    }

    if (!source.equalTo(other.getSource())){
      return false;
    }

    if (maxAgeSeconds != other.maxAgeSeconds){
      return false;
    }

    // Create a map of all fields in this feature set
    Map<String, Field> fields = new HashMap<>();

    for (Field e : entities){
      fields.putIfAbsent(e.getName(), e);
    }

    for (Field f : features){
      fields.putIfAbsent(f.getName(), f);
    }

    // Ensure map size is consistent with existing fields
    if (fields.size() != other.features.size() + other.entities.size())
    {
      return false;
    }

    // Ensure the other entities and fields exist in the field map
    for (Field e : other.entities){
      if(!fields.containsKey(e.getName())){
        return false;
      }
      if (!e.equals(fields.get(e.getName()))){
        return false;
      }
    }

    for (Field f : features){
      if(!fields.containsKey(f.getName())){
        return false;
      }
      if (!f.equals(fields.get(f.getName()))){
        return false;
      }
    }

    return true;
  }

  @Override
  public int compareTo(FeatureSet o) {
    return Integer.compare(version, o.version);
  }
}
