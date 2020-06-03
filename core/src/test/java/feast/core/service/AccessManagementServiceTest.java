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

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import feast.core.dao.ProjectRepository;
import feast.core.model.Project;
import java.util.Optional;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

public class AccessManagementServiceTest {
  @Rule public ExpectedException expectedException = ExpectedException.none();
  // mocks
  @Mock private ProjectRepository projectRepository;
  // dummy models
  private Project defaultProject;
  private Project testProject;

  // test target
  private AccessManagementService accessService;

  @Before
  public void setup() {
    initMocks(this);
    // setup dummy models for testing
    this.defaultProject = new Project(Project.DEFAULT_NAME);
    this.testProject = new Project("project");
    // setup test target
    when(this.projectRepository.existsById(Project.DEFAULT_NAME)).thenReturn(false);
    this.accessService = new AccessManagementService(this.projectRepository);
  }

  @Test
  public void testDefaultProjectCreateInConstructor() {
    verify(this.projectRepository).saveAndFlush(this.defaultProject);
  }

  @Test
  public void testArchiveProject() {
    when(this.projectRepository.findById("project")).thenReturn(Optional.of(this.testProject));
    this.accessService.archiveProject("project");
    this.testProject.setArchived(true);
    verify(this.projectRepository).saveAndFlush(this.testProject);
    // reset archived flag
    this.testProject.setArchived(false);
  }

  @Test
  public void shouldNotArchiveDefaultProject() {
    expectedException.expect(IllegalArgumentException.class);
    this.accessService.archiveProject(Project.DEFAULT_NAME);
  }
}
