package feast.core.service;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import feast.core.dao.ProjectRepository;
import feast.core.dao.UserRepository;
import feast.core.model.Project;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;

public class AccessManagementServiceTest {

  @Mock
  private ProjectRepository projectRepository;

  @Mock
  private UserRepository userRepository;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private AccessManagementService accessManagementService;

  @Before
  public void setUp() {
    initMocks(this);
    accessManagementService = new AccessManagementService(projectRepository, userRepository);
  }

  @Test
  public void shouldCreateProjectIfItDoesntExist() {
    String project_name = "project1";
    Project project = new Project(project_name);

    when(projectRepository.saveAndFlush(any(Project.class))).thenReturn(project);
    accessManagementService.createProject(project_name);
    verify(projectRepository, times(1)).saveAndFlush(any());
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldNotCreateProjectIfItExist(){
    String project_name = "project1";

    when(projectRepository.existsById(project_name)).thenReturn(true);
    accessManagementService.createProject(project_name);
  }

  @Test
  public void archiveProject() {
  }

  @Test
  public void listProjects() {
  }

  @Test
  public void listMembers() {
  }

  @Test
  public void addMember() {
  }

  @Test
  public void removeMember() {
  }
}