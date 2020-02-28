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
import javax.persistence.*;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(name = "projects")
public class Project {

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
  private Set<FeatureSet> featureSets;

  @ManyToMany(mappedBy = "projects", fetch = FetchType.EAGER)
  private Set<User> projectMembers;

  public Project() {
    super();
  }

  public Project(String name) {
    this.name = name;
    this.featureSets = new HashSet<>();
    this.projectMembers = new HashSet<>();
  }

  public void addFeatureSet(FeatureSet featureSet) {
    featureSet.setProject(this);
    featureSets.add(featureSet);
  }

  public void addUser(User user){
    user.addProject(this);
    projectMembers.add(user);
  }

  public void removeUser(User user){
    user.removeProject(this);
    projectMembers.remove(user);
  }

  public boolean getUser(User user){
    if (projectMembers.contains(user)) {
      return true;
    }
    return false;
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
