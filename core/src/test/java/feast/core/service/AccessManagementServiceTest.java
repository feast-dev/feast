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
package feast.core.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import feast.core.auth.authorization.AuthorizationProvider;
import feast.core.config.FeastProperties;
import feast.core.config.FeastProperties.SecurityProperties;
import feast.core.dao.ProjectRepository;
import feast.core.model.Project;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

public class AccessManagementServiceTest {

  @Mock private ProjectRepository projectRepository;

  @Rule public final ExpectedException expectedException = ExpectedException.none();

  private AccessManagementService accessManagementService;

  @Before
  public void setUp() {
    initMocks(this);
    projectRepository = mock(ProjectRepository.class);
    FeastProperties.SecurityProperties.AuthorizationProperties authProp =
        new FeastProperties.SecurityProperties.AuthorizationProperties();
    authProp.setEnabled(false);
    FeastProperties.SecurityProperties sp = new SecurityProperties();
    sp.setAuthorization(authProp);
    FeastProperties feastProperties = new FeastProperties();
    feastProperties.setSecurity(sp);
    accessManagementService =
        new AccessManagementService(
            feastProperties, projectRepository, mock(AuthorizationProvider.class));
  }

  @Test
  public void testDefaultProjectCreateInConstructor() {
    verify(this.projectRepository).saveAndFlush(new Project(Project.DEFAULT_NAME));
  }

  @Test
  public void shouldCreateProjectIfItDoesntExist() {
    String projectName = "project1";
    Project project = new Project(projectName);
    when(projectRepository.saveAndFlush(any(Project.class))).thenReturn(project);
    accessManagementService.createProject(projectName);
    verify(projectRepository, times(1)).saveAndFlush(any());
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldNotCreateProjectIfItExist() {
    String projectName = "project1";
    when(projectRepository.existsById(projectName)).thenReturn(true);
    accessManagementService.createProject(projectName);
  }

  @Test
  public void shouldArchiveProjectIfItExists() {
    String projectName = "project1";
    when(projectRepository.findById(projectName)).thenReturn(Optional.of(new Project(projectName)));
    accessManagementService.archiveProject(projectName);
    verify(projectRepository, times(1)).saveAndFlush(any(Project.class));
  }

  @Test
  public void shouldNotArchiveDefaultProject() {
    expectedException.expect(IllegalArgumentException.class);
    this.accessManagementService.archiveProject(Project.DEFAULT_NAME);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldNotArchiveProjectIfItIsAlreadyArchived() {
    String projectName = "project1";
    when(projectRepository.findById(projectName)).thenReturn(Optional.empty());
    accessManagementService.archiveProject(projectName);
  }

  @Test
  public void shouldListProjects() {
    String projectName = "project1";
    Project project = new Project(projectName);
    List<Project> expected = Arrays.asList(project);
    when(projectRepository.findAllByArchivedIsFalse()).thenReturn(expected);
    List<Project> actual = accessManagementService.listProjects();
    Assert.assertEquals(expected, actual);
  }
}
