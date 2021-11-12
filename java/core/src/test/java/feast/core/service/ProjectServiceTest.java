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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

import feast.core.dao.ProjectRepository;
import feast.core.model.Project;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class ProjectServiceTest {

  @Mock private ProjectRepository projectRepository;
  private ProjectService projectService;

  @Before
  public void setUp() {
    initMocks(this);
    projectRepository = mock(ProjectRepository.class);
    projectService = new ProjectService(projectRepository);
  }

  @Test
  public void shouldCreateProjectIfItDoesntExist() {
    String projectName = "project1";
    Project project = new Project(projectName);
    when(projectRepository.saveAndFlush(project)).thenReturn(project);
    projectService.createProject(projectName);
    verify(projectRepository, times(1)).saveAndFlush(project);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldNotCreateProjectIfItExist() {
    String projectName = "project1";
    when(projectRepository.existsById(projectName)).thenReturn(true);
    projectService.createProject(projectName);
  }

  @Test
  public void shouldArchiveProjectIfItExists() {
    String projectName = "project1";
    Project project = new Project(projectName);
    when(projectRepository.findById(projectName)).thenReturn(Optional.of(project));
    projectService.archiveProject(projectName);
    verify(projectRepository, times(1)).saveAndFlush(project);
  }

  @Test
  public void shouldNotArchiveDefaultProject() {
    assertThrows(
        IllegalArgumentException.class,
        () -> this.projectService.archiveProject(Project.DEFAULT_NAME));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldNotArchiveProjectIfItIsAlreadyArchived() {
    String projectName = "project1";
    when(projectRepository.findById(projectName)).thenReturn(Optional.empty());
    projectService.archiveProject(projectName);
  }

  @Test
  public void shouldListProjects() {
    String projectName = "project1";
    Project project = new Project(projectName);
    List<Project> expected = Arrays.asList(project);
    when(projectRepository.findAllByArchivedIsFalse()).thenReturn(expected);
    List<Project> actual = projectService.listProjects();
    Assert.assertEquals(expected, actual);
  }
}
