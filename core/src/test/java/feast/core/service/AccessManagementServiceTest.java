package feast.core.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import feast.core.dao.ProjectRepository;
import feast.core.dao.UserRepository;
import feast.core.model.Project;
import feast.core.model.User;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
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
    when(projectRepository.findById(projectName))
        .thenReturn(Optional.of(new Project(projectName)));
    accessManagementService.archiveProject(projectName);
    verify(projectRepository, times(1)).saveAndFlush(any(Project.class));
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

  @Test
  public void shouldListMembersWhenProjectExists() {
    String projectName = "project1";
    User user = new User("user1");
    Set<User> expected = new HashSet<>(Arrays.asList(user));
    Project mockProject = mock(Project.class);
    when(projectRepository.findById(projectName)).thenReturn(Optional.of(mockProject));
    when(mockProject.getProjectMembers()).thenReturn(new HashSet<>(Arrays.asList(user)));
    Set<User> actual = accessManagementService.listMembers(projectName);
    Assert.assertEquals(expected, actual);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldNotListMembersWhenProjectDoesntExist() {
    String projectName = "project1";
    accessManagementService.listMembers(projectName);
  }

  @Test
  public void shouldAddMemberWhenProjectExists() {
    String projectName = "project1";
    String userName = "user1";
    Project mockProject = mock(Project.class);
    when(projectRepository.findById(projectName)).thenReturn(Optional.of(mockProject));
    when(userRepository.saveAndFlush(any(User.class))).thenReturn(new User(userName));
    accessManagementService.addMember(userName, projectName);
    verify(userRepository, times(1)).saveAndFlush(any());
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldNotAddMemberWhenUserAlreadyExists() {
    String projectName = "project1";
    String userName = "user1";
    Project mockProject = mock(Project.class);
    when(projectRepository.findById(projectName)).thenReturn(Optional.of(mockProject));
    when(userRepository.existsUserByName(userName)).thenReturn(true);
    accessManagementService.addMember(userName, projectName);
  }

  @Test
  public void shouldRemoveMemberIfRegisteredAsProjectMember() {
    String projectName = "project1";
    String userName = "user1";
    Project mockProject = mock(Project.class);
    when(projectRepository.findById(projectName)).thenReturn(Optional.of(mockProject));
    when(userRepository.findByName(userName)).thenReturn(Optional.of(new User(userName)));
    accessManagementService.removeMember(userName, projectName);
    verify(userRepository, times(1)).saveAndFlush(any(User.class));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldRemoveMemberIfUserDoesntExist() {
    String projectName = "project1";
    String userName = "user1";
    Project mockProject = mock(Project.class);
    when(projectRepository.findById(projectName)).thenReturn(Optional.of(mockProject));
    when(userRepository.findByName(userName)).thenReturn(Optional.empty());
    accessManagementService.removeMember(userName, projectName);
  }
}
