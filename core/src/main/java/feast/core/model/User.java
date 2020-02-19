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
import java.util.Set;

@Getter
@Setter
@Entity
@Table(name = "users")
public class User {

  // User Id
  @Id
  @Column(name = "id", nullable = false, unique = true)
  private String id;

  // Name of the user
  @Column(name = "name", nullable = false)
  private String name;

  //Password of the user
  @Column(name = "password_sha", nullable = false)
  private String password_sha;

  @ManyToMany
  @JoinTable(
      name = "projects_members",
      joinColumns = @JoinColumn(name = "user_id"),
      inverseJoinColumns = @JoinColumn(name = "project_name")
  )
  Set<Project> projects;

  public User() {
    super();
  }

  public User(String id, String name, String password_sha) {
    this.id = id;
    this.name = name;
    this.password_sha = password_sha;
  }
}
