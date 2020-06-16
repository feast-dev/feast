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

import feast.core.auth.authorization.AuthorizationProvider;
import feast.core.auth.authorization.AuthorizationResult;
import feast.core.config.FeastProperties;
import feast.core.config.FeastProperties.SecurityProperties;
import feast.core.dao.ProjectRepository;
import feast.core.model.Project;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ProjectService {

  private SecurityProperties securityProperties;
  private AuthorizationProvider authorizationProvider;
  private ProjectRepository projectRepository;

  public ProjectService(
      FeastProperties feastProperties,
      ProjectRepository projectRepository,
      AuthorizationProvider authorizationProvider) {
    this.projectRepository = projectRepository;
    this.authorizationProvider = authorizationProvider;
    this.securityProperties = feastProperties.getSecurity();
  }

  @Autowired
  public ProjectService(
      FeastProperties feastProperties,
      ProjectRepository projectRepository,
      ObjectProvider<AuthorizationProvider> authorizationProvider) {
    this.projectRepository = projectRepository;
    // create default project if it does not yet exist.
    if (!projectRepository.existsById(Project.DEFAULT_NAME)) {
      this.createProject(Project.DEFAULT_NAME);
    }
    this.authorizationProvider = authorizationProvider.getIfUnique();
    this.securityProperties = feastProperties.getSecurity();
  }

  /**
   * Creates a project
   *
   * @param name Name of project to be created
   */
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
  public void archiveProject(String name) {
    Optional<Project> project = projectRepository.findById(name);
    if (!project.isPresent()) {
      throw new IllegalArgumentException(String.format("Could not find project: \"%s\"", name));
    }
    if (name.equals(Project.DEFAULT_NAME)) {
      throw new UnsupportedOperationException("Archiving the default project is not allowed.");
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

  /**
   * Determine whether a user belongs to a Project
   *
   * @param securityContext User's Spring Security Context. Used to identify user.
   * @param project Name of the project for which membership should be tested.
   */
  public void checkIfProjectMember(SecurityContext securityContext, String project) {
    Authentication authentication = securityContext.getAuthentication();
    if (!this.securityProperties.getAuthorization().isEnabled()) {
      return;
    }
    AuthorizationResult result = this.authorizationProvider.checkAccess(project, authentication);
    if (!result.isAllowed()) {
      throw new AccessDeniedException(result.getFailureReason().orElse("AccessDenied"));
    }
  }
}
