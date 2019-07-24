package feast.core.job.flink;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import feast.core.config.ImportJobDefaults;
import feast.specs.ImportJobSpecsProto.ImportJobSpecs;
import feast.specs.ImportSpecProto.ImportSpec;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import org.apache.flink.client.cli.CliFrontend;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class FlinkJobManagerTest {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();
  @Mock
  private CliFrontend flinkCli;
  @Mock
  private FlinkRestApi flinkRestApi;
  private FlinkJobConfig config;
  private ImportJobDefaults defaults;
  private FlinkJobManager flinkJobManager;
  private Path workspace;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    config = new FlinkJobConfig("localhost:8081", "/etc/flink/conf");
    workspace = Paths.get(tempFolder.newFolder().toString());
    defaults =
        ImportJobDefaults.builder().
            runner("FlinkRunner").importJobOptions("{\"key\":\"value\"}")
            .executable("ingestion.jar")
            .workspace(workspace.toString()).build();

    flinkJobManager = new FlinkJobManager(flinkCli, config, flinkRestApi, defaults);
  }

  @Test
  public void shouldPassCorrectArgumentForSubmittingJob() throws IOException {
    FlinkJobList response = new FlinkJobList();
    response.setJobs(Collections.singletonList(new FlinkJob("1234", "job1", "RUNNING")));
    when(flinkRestApi.getJobsOverview()).thenReturn(response);

    String jobName = "importjob";

    flinkJobManager.startJob(jobName, Paths.get("/tmp/foobar"));
    String[] expected =
        new String[]{
            "run",
            "-d",
            "-m",
            config.getMasterUrl(),
            defaults.getExecutable(),
            "--jobName=" + jobName,
            "--runner=FlinkRunner",
            "--workspace=/tmp/foobar",
            "--key=value"
        };

    ArgumentCaptor<String[]> argumentCaptor = ArgumentCaptor.forClass(String[].class);
    verify(flinkCli).parseParameters(argumentCaptor.capture());

    String[] actual = argumentCaptor.getValue();
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldReturnFlinkJobId() {
    FlinkJobList response = new FlinkJobList();
    String flinkJobId = "1234";
    String jobName = "importjob";
    response.setJobs(Collections.singletonList(new FlinkJob(flinkJobId, jobName, "RUNNING")));
    when(flinkRestApi.getJobsOverview()).thenReturn(response);

    String jobId = flinkJobManager.startJob(jobName, workspace);

    assertThat(jobId, equalTo(flinkJobId));
  }

  @Test
  public void shouldPassCorrectArgumentForStoppingJob() {
    String jobId = "1234";

    flinkJobManager.abortJob(jobId);

    String[] expected = new String[]{
        "cancel",
        "-m",
        config.getMasterUrl(),
        jobId
    };

    ArgumentCaptor<String[]> argumentCaptor = ArgumentCaptor.forClass(String[].class);
    verify(flinkCli).parseParameters(argumentCaptor.capture());

    String[] actual = argumentCaptor.getValue();
    assertThat(actual, equalTo(expected));
  }
}
