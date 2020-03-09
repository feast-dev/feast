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
package feast.core.service;

import feast.core.dao.ProjectRepository;
import feast.core.model.Project;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
public class AccessManagementService {

  private ProjectRepository projectRepository;

  @Autowired
  public AccessManagementService(ProjectRepository projectRepository) {
    this.projectRepository = projectRepository;
  }

  /**
   * Creates a project
   *
   * @param name Name of project to be created
   */
  @Transactional
  public void createProject(String name) {
    if (projectRepository.existsById(name)) {
      throw new IllegalArgumentException(String.format("Project already exists: %s", name));
    }
    Project project = new Project(name);
    projectRepository.saveAndFlush(project);
  }

  /**
   * Archives a project
   *
   * @param name Name of the project to be archived
   */
  @Transactional
  public void archiveProject(String name) {
    Optional<Project> project = projectRepository.findById(name);
    if (!project.isPresent()) {
      throw new IllegalArgumentException(String.format("Could not find project: \"%s\"", name));
    }
    Project p = project.get();
    p.setArchived(true);
    projectRepository.saveAndFlush(p);
  }

  /**
   * List all active projects
   *
   * @return List of active projects
   */
  public List<Project> listProjects() {
    return projectRepository.findAllByArchivedIsFalse();
  }
}
