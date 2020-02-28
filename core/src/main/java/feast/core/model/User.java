/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
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

import lombok.Getter;
import lombok.Setter;

import javax.persistence.*;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

@Getter
@Setter
@Entity
@Table(name = "users")
public class User {

  // User Id
  @Id
  @GeneratedValue(strategy = GenerationType.SEQUENCE)
  @Column(name = "id", nullable = false, unique = true)
  private Integer id;

  // Name of the user
  @Column(name = "name", nullable = false)
  private String name;

  //Password of the user
  @Column(name = "password_sha")
  private String password_sha;

  @ManyToMany(fetch = FetchType.EAGER)
  @JoinTable(
      name = "projects_members",
      joinColumns = @JoinColumn(name = "user_id"),
      inverseJoinColumns = @JoinColumn(name = "project_name")
  )
  Set<Project> projects;

  public User(String name) {
    this.name = name;
    this.projects = new HashSet<>();
  }

  public void addProject(Project project){
    projects.add(project);
    this.setProjects(projects);
  }

  public void removeProject(Project project){
    projects.remove(project);
    this.setProjects(projects);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    User field = (User) o;
    return name.equals(field.getName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), name);
  }
}
