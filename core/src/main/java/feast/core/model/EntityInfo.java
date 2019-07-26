/*
 * Copyright 2018 The Feast Authors
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
 *
 */

package feast.core.model;

import feast.core.UIServiceProto.UIServiceTypes.EntityDetail;
import feast.specs.EntitySpecProto.EntitySpec;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.*;
import java.util.List;

import static feast.core.util.TypeConversion.convertTagStringToList;
import static feast.core.util.TypeConversion.convertTimestamp;

/**
 * A row in the registry storing information about a single Entity, including its relevant metadata.
 */
@AllArgsConstructor
@Getter
@Setter
@Entity
@Table(name = "entities")
public class EntityInfo extends AbstractTimestampEntity {

  @Id
  @Column(name = "name", nullable = false)
  private String name;

  @Column(name = "description", nullable = false)
  private String description;

  @Column(name = "tags")
  private String tags;

  @JoinColumn(name = "topic")
  private FeatureStreamTopic topic;

  @ManyToMany(mappedBy = "entities")
  private List<JobInfo> jobs;

  @Column(name = "enabled")
  private boolean enabled = true;

  public EntityInfo() {
    super();
  }

  public EntityInfo(EntitySpec spec) {
    this.name = spec.getName();
    this.description = spec.getDescription();
    this.tags = String.join(",", spec.getTagsList());
  }

  /** Get the entity spec associated with this record. */
  public EntitySpec getEntitySpec() {
    return EntitySpec.newBuilder()
        .setName(name)
        .setDescription(description)
        .addAllTags(convertTagStringToList(tags))
        .build();
  }

  /** Get the entity detail containing both spec and metadata, associated with this record. */
  public EntityDetail getEntityDetail() {
    return EntityDetail.newBuilder()
        .setSpec(this.getEntitySpec())
        .setLastUpdated(convertTimestamp(this.getLastUpdated()))
        .build();
  }

  /**
   * Updates the entity info with specifications from the incoming entity spec.
   *
   * @param update new entity spec
   */
  public void update(EntitySpec update) {
    this.description = update.getDescription();
    this.tags = String.join(",", update.getTagsList());
  }
}
