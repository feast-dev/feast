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
import com.google.protobuf.Timestamp;
import feast.core.FeatureSetProto;
import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSetMeta;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.FeatureSetProto.FeatureSetStatus;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.types.ValueProto.ValueType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.persistence.CascadeType;
import javax.persistence.CollectionTable;
import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;

@Getter
@Setter
@Entity
@Table(name = "feature_sets")
public class FeatureSet extends AbstractTimestampEntity implements Comparable<FeatureSet> {

  // Id of the featureSet, defined as project/feature_set_name:feature_set_version
  @Id
  @Column(name = "id", nullable = false, unique = true)
  private String id;

  // Name of the featureSet
  @Column(name = "name", nullable = false)
  private String name;

  // Version of the featureSet
  @Column(name = "version")
  private int version;

  // Project that this featureSet belongs to
  @ManyToOne(fetch = FetchType.LAZY)
  @JoinColumn(name = "project_name")
  private Project project;

  // Max allowed staleness for features in this featureSet.
  @Column(name = "max_age")
  private long maxAgeSeconds;

  // Entity fields inside this feature set
  @ElementCollection(fetch = FetchType.EAGER)
  @CollectionTable(name = "entities", joinColumns = @JoinColumn(name = "feature_set_id"))
  @Fetch(FetchMode.SUBSELECT)
  private Set<Field> entities;

  // Feature fields inside this feature set
  @ElementCollection(fetch = FetchType.EAGER)
  @CollectionTable(
      name = "features",
      joinColumns = @JoinColumn(name = "feature_set_id"),
      uniqueConstraints = @UniqueConstraint(columnNames = {"name", "project", "version"}))
  @Fetch(FetchMode.SUBSELECT)
  private Set<Field> features;

  // Source on which feature rows can be found
  @ManyToOne(cascade = CascadeType.ALL, fetch = FetchType.EAGER)
  @JoinColumn(name = "source")
  private Source source;

  // Status of the feature set
  @Column(name = "status")
  private String status;

  public FeatureSet() {
    super();
  }

  public FeatureSet(
      String name,
      String project,
      int version,
      long maxAgeSeconds,
      List<Field> entities,
      List<Field> features,
      Source source,
      FeatureSetStatus status) {
    this.maxAgeSeconds = maxAgeSeconds;
    this.source = source;
    this.status = status.toString();
    this.entities = new HashSet<>();
    this.features = new HashSet<>();
    this.name = name;
    this.project = new Project(project);
    this.version = version;
    this.setId(project, name, version);
    addEntities(entities);
    addFeatures(features);
  }

  private void setId(String project, String name, int version) {
    this.id = project + "/" + name + ":" + version;
  }

  public void setVersion(int version) {
    this.version = version;
    this.setId(getProjectName(), getName(), version);
  }

  public void setName(String name) {
    this.name = name;
    this.setId(getProjectName(), name, getVersion());
  }

  private String getProjectName() {
    if (getProject() != null) {
      return getProject().getName();
    } else {
      return "";
    }
  }

  public void setProject(Project project) {
    this.project = project;
    this.setId(project.getName(), getName(), getVersion());
  }

  public static FeatureSet fromProto(FeatureSetProto.FeatureSet featureSetProto) {
    FeatureSetSpec featureSetSpec = featureSetProto.getSpec();
    Source source = Source.fromProto(featureSetSpec.getSource());

    List<Field> features = new ArrayList<>();
    for (FeatureSpec feature : featureSetSpec.getFeaturesList()) {
      features.add(new Field(feature.getName(), feature.getValueType()));
    }

    List<Field> entities = new ArrayList<>();
    for (EntitySpec entity : featureSetSpec.getEntitiesList()) {
      entities.add(new Field(entity.getName(), entity.getValueType()));
    }

    return new FeatureSet(
        featureSetProto.getSpec().getName(),
        featureSetProto.getSpec().getProject(),
        featureSetProto.getSpec().getVersion(),
        featureSetSpec.getMaxAge().getSeconds(),
        entities,
        features,
        source,
        featureSetProto.getMeta().getStatus());
  }

  public void addEntities(List<Field> fields) {
    for (Field field : fields) {
      addEntity(field);
    }
  }

  public void addEntity(Field field) {
    field.setProject(this.project.getName());
    field.setVersion(this.getVersion());
    entities.add(field);
  }

  public void addFeatures(List<Field> fields) {
    for (Field field : fields) {
      addFeature(field);
    }
  }

  public void addFeature(Field field) {
    field.setProject(this.project.getName());
    field.setVersion(this.getVersion());
    features.add(field);
  }

  public FeatureSetProto.FeatureSet toProto() {
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
    FeatureSetMeta.Builder meta =
        FeatureSetMeta.newBuilder()
            .setCreatedTimestamp(
                Timestamp.newBuilder().setSeconds(super.getCreated().getTime() / 1000L))
            .setStatus(FeatureSetStatus.valueOf(status));

    FeatureSetSpec.Builder spec =
        FeatureSetSpec.newBuilder()
            .setName(getName())
            .setVersion(getVersion())
            .setProject(project.getName())
            .setMaxAge(Duration.newBuilder().setSeconds(maxAgeSeconds))
            .addAllEntities(entitySpecs)
            .addAllFeatures(featureSpecs)
            .setSource(source.toProto());

    return FeatureSetProto.FeatureSet.newBuilder().setMeta(meta).setSpec(spec).build();
  }

  /**
   * Checks if the given featureSet's schema and source has is different from this one.
   *
   * @param other FeatureSet to compare to
   * @return boolean denoting if the source or schema have changed.
   */
  public boolean equalTo(FeatureSet other) {
    if (!getName().equals(other.getName())) {
      return false;
    }

    if (!project.getName().equals(other.project.getName())) {
      return false;
    }

    if (!source.equalTo(other.getSource())) {
      return false;
    }

    if (maxAgeSeconds != other.maxAgeSeconds) {
      return false;
    }

    // Create a map of all fields in this feature set
    Map<String, Field> fields = new HashMap<>();

    for (Field e : entities) {
      fields.putIfAbsent(e.getName(), e);
    }

    for (Field f : features) {
      fields.putIfAbsent(f.getName(), f);
    }

    // Ensure map size is consistent with existing fields
    if (fields.size() != other.getFeatures().size() + other.getEntities().size()) {
      return false;
    }

    // Ensure the other entities and features exist in the field map
    for (Field e : other.getEntities()) {
      if (!fields.containsKey(e.getName())) {
        return false;
      }
      if (!e.equals(fields.get(e.getName()))) {
        return false;
      }
    }

    for (Field f : other.getFeatures()) {
      if (!fields.containsKey(f.getName())) {
        return false;
      }
      if (!f.equals(fields.get(f.getName()))) {
        return false;
      }
    }

    return true;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder hcb = new HashCodeBuilder();
    hcb.append(project.getName());
    hcb.append(getName());
    hcb.append(getVersion());
    return hcb.toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof FeatureSet)) {
      return false;
    }
    return this.equalTo(((FeatureSet) obj));
  }

  @Override
  public int compareTo(FeatureSet o) {
    return Integer.compare(getVersion(), o.getVersion());
  }
}
