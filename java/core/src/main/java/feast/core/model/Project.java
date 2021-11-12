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

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(name = "projects")
public class Project {
  public static final String DEFAULT_NAME = "default";

  // Name of the project
  @Id
  @Column(name = "name", nullable = false, unique = true)
  private String name;

  // Flag to set whether the project has been archived
  @Column(name = "archived", nullable = false)
  private boolean archived;

  @OneToMany(
      cascade = CascadeType.ALL,
      fetch = FetchType.EAGER,
      orphanRemoval = true,
      mappedBy = "project")
  private Set<EntityV2> entities;

  @OneToMany(
      cascade = CascadeType.ALL,
      fetch = FetchType.EAGER,
      orphanRemoval = true,
      mappedBy = "project")
  private Set<FeatureTable> featureTables;

  public Project() {
    super();
  }

  public Project(String name) {
    this.name = name;
    this.entities = new HashSet<>();
    this.featureTables = new HashSet<>();
  }

  public void addEntity(EntityV2 entity) {
    entity.setProject(this);
    entities.add(entity);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Project field = (Project) o;
    return name.equals(field.getName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), name);
  }
}
