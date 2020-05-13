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

import com.google.common.collect.Sets;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import feast.core.FeatureSetProto;
import feast.core.FeatureSetProto.*;
import feast.core.util.TypeConversion;
import java.util.*;
import java.util.stream.Collectors;
import javax.persistence.*;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.tensorflow.metadata.v0.*;

@Getter
@Setter
@javax.persistence.Entity
@Table(
    name = "feature_sets",
    uniqueConstraints = @UniqueConstraint(columnNames = {"name", "project_name"}))
public class FeatureSet extends AbstractTimestampEntity {

  // Id of the featureSet, defined as project/feature_set_name:feature_set_version
  @Id @GeneratedValue private long id;

  // Name of the featureSet
  @Column(name = "name", nullable = false)
  private String name;

  // Project that this featureSet belongs to
  @ManyToOne(fetch = FetchType.LAZY)
  @JoinColumn(name = "project_name")
  private Project project;

  // Max allowed staleness for features in this featureSet.
  @Column(name = "max_age")
  private long maxAgeSeconds;

  // Entity fields inside this feature set
  @OneToMany(
      mappedBy = "featureSet",
      cascade = CascadeType.ALL,
      fetch = FetchType.EAGER,
      orphanRemoval = true)
  private Set<Entity> entities;

  // Feature fields inside this feature set
  @OneToMany(
      mappedBy = "featureSet",
      cascade = CascadeType.ALL,
      fetch = FetchType.EAGER,
      orphanRemoval = true)
  private Set<Feature> features;

  // Source on which feature rows can be found
  @ManyToOne(cascade = CascadeType.ALL, fetch = FetchType.EAGER)
  @JoinColumn(name = "source")
  private Source source;

  // Status of the feature set
  @Enumerated(EnumType.STRING)
  @Column(name = "status")
  private FeatureSetStatus status;

  // User defined metadata
  @Column(name = "labels", columnDefinition = "text")
  private String labels;

  public FeatureSet() {
    super();
  }

  public FeatureSet(
      String name,
      String project,
      long maxAgeSeconds,
      List<Entity> entities,
      List<Feature> features,
      Source source,
      Map<String, String> labels,
      FeatureSetStatus status) {
    this.maxAgeSeconds = maxAgeSeconds;
    this.source = source;
    this.status = status;
    this.entities = new HashSet<>();
    this.features = new HashSet<>();
    this.name = name;
    this.project = new Project(project);
    this.labels = TypeConversion.convertMapToJsonString(labels);
    addEntities(entities);
    addFeatures(features);
  }

  public void setName(String name) {
    this.name = name;
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
  }

  public static FeatureSet fromProto(FeatureSetProto.FeatureSet featureSetProto) {
    FeatureSetSpec featureSetSpec = featureSetProto.getSpec();
    Source source = Source.fromProto(featureSetSpec.getSource());

    List<Feature> featureSpecs = new ArrayList<>();
    for (FeatureSpec featureSpec : featureSetSpec.getFeaturesList()) {
      featureSpecs.add(Feature.fromProto(featureSpec));
    }

    List<Entity> entitySpecs = new ArrayList<>();
    for (EntitySpec entitySpec : featureSetSpec.getEntitiesList()) {
      entitySpecs.add(Entity.fromProto(entitySpec));
    }

    return new FeatureSet(
        featureSetProto.getSpec().getName(),
        featureSetProto.getSpec().getProject(),
        featureSetSpec.getMaxAge().getSeconds(),
        entitySpecs,
        featureSpecs,
        source,
        featureSetProto.getSpec().getLabelsMap(),
        featureSetProto.getMeta().getStatus());
  }

  // Updates the existing feature set from a proto.
  public void updateFromProto(FeatureSetProto.FeatureSet featureSetProto)
      throws InvalidProtocolBufferException {
    FeatureSetSpec spec = featureSetProto.getSpec();
    if (this.toProto().getSpec().equals(spec)) {
      return;
    }

    // 1. validate
    // 1a. check no change to identifiers
    if (!name.equals(spec.getName())) {
      throw new IllegalArgumentException(
          String.format("Given feature set name %s does not match name %s.", spec.getName(), name));
    }
    if (!project.getName().equals(spec.getProject())) {
      throw new IllegalArgumentException(
          String.format(
              "You are attempting to change the project of feature set %s from %s to %s. This isn't allowed. Please create a new feature set under the desired project.",
              spec.getName(), project, spec.getProject()));
    }

    Set<EntitySpec> existingEntities =
        entities.stream().map(Entity::toProto).collect(Collectors.toSet());

    // 1b. check no change to entities
    if (!Sets.newHashSet(spec.getEntitiesList()).equals(existingEntities)) {
      throw new IllegalArgumentException(
          String.format(
              "You are attempting to change the entities of this feature set: Given set of entities \n{%s}\n does not match existing set of entities\n {%s}. This isn't allowed. Please create a new feature set. ",
              spec.getEntitiesList(), existingEntities));
    }

    // 4. Update max age and source.
    maxAgeSeconds = spec.getMaxAge().getSeconds();
    source = Source.fromProto(spec.getSource());

    Map<String, FeatureSpec> updatedFeatures =
        spec.getFeaturesList().stream().collect(Collectors.toMap(FeatureSpec::getName, fs -> fs));

    // 3. Tombstone features that are gone, update features that have changed
    for (Feature existingFeature : features) {
      String existingFeatureName = existingFeature.getName();
      FeatureSpec updatedFeatureSpec = updatedFeatures.get(existingFeatureName);
      if (updatedFeatureSpec == null) {
        existingFeature.archive();
      } else {
        existingFeature.updateFromProto(updatedFeatureSpec);
        updatedFeatures.remove(existingFeatureName);
      }
    }

    // 4. Add new features
    for (FeatureSpec featureSpec : updatedFeatures.values()) {
      Feature newFeature = Feature.fromProto(featureSpec);
      addFeature(newFeature);
    }
  }

  public void addEntities(List<Entity> entities) {
    for (Entity entity : entities) {
      addEntity(entity);
    }
  }

  public void addEntity(Entity entity) {
    entity.setFeatureSet(this);
    entities.add(entity);
  }

  public void addFeatures(List<Feature> features) {
    for (Feature feature : features) {
      addFeature(feature);
    }
  }

  public void addFeature(Feature feature) {
    feature.setFeatureSet(this);
    features.add(feature);
  }

  public FeatureSetProto.FeatureSet toProto() throws InvalidProtocolBufferException {
    List<EntitySpec> entitySpecs = new ArrayList<>();
    for (Entity entityField : entities) {
      entitySpecs.add(entityField.toProto());
    }

    List<FeatureSpec> featureSpecs = new ArrayList<>();
    for (Feature featureField : features) {
      if (!featureField.isArchived()) {
        featureSpecs.add(featureField.toProto());
      }
    }

    FeatureSetMeta.Builder meta =
        FeatureSetMeta.newBuilder()
            .setCreatedTimestamp(
                Timestamp.newBuilder().setSeconds(super.getCreated().getTime() / 1000L))
            .setStatus(status);

    FeatureSetSpec.Builder spec =
        FeatureSetSpec.newBuilder()
            .setName(getName())
            .setProject(project.getName())
            .setMaxAge(Duration.newBuilder().setSeconds(maxAgeSeconds))
            .addAllEntities(entitySpecs)
            .addAllFeatures(featureSpecs)
            .putAllLabels(TypeConversion.convertJsonStringToMap(labels))
            .setSource(source.toProto());

    return FeatureSetProto.FeatureSet.newBuilder().setMeta(meta).setSpec(spec).build();
  }

  @Override
  public int hashCode() {
    HashCodeBuilder hcb = new HashCodeBuilder();
    hcb.append(project.getName());
    hcb.append(getName());
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

    FeatureSet other = (FeatureSet) obj;
    if (!getName().equals(other.getName())) {
      return false;
    }

    if (!getLabels().equals(other.getLabels())) {
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
    Map<String, Entity> entitiesMap = new HashMap<>();
    Map<String, Feature> featuresMap = new HashMap<>();

    for (Entity e : entities) {
      entitiesMap.putIfAbsent(e.getName(), e);
    }

    for (Feature f : features) {
      featuresMap.putIfAbsent(f.getName(), f);
    }

    // Ensure map size is consistent with existing fields
    if (entitiesMap.size() != other.getEntities().size()) {
      return false;
    }
    if (featuresMap.size() != other.getFeatures().size()) {
      return false;
    }

    // Ensure the other entities and features exist in the field map
    for (Entity e : other.getEntities()) {
      if (!entitiesMap.containsKey(e.getName())) {
        return false;
      }
      if (!e.equals(entitiesMap.get(e.getName()))) {
        return false;
      }
    }

    for (Feature f : other.getFeatures()) {
      if (!featuresMap.containsKey(f.getName())) {
        return false;
      }
      if (!f.equals(featuresMap.get(f.getName()))) {
        return false;
      }
    }

    return true;
  }
}
