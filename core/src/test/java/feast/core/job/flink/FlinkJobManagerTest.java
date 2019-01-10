package feast.core.job.flink;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import feast.core.config.ImportJobDefaults;
import feast.specs.ImportSpecProto.ImportSpec;
import java.util.Collections;
import org.apache.flink.client.cli.CliFrontend;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class FlinkJobManagerTest {
  @Mock private CliFrontend flinkCli;
  @Mock private FlinkRestApi flinkRestApi;

  private FlinkJobConfig config;
  private ImportJobDefaults defaults;
  private FlinkJobManager flinkJobManager;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    config = new FlinkJobConfig("localhost:8081", "/etc/flink/conf");
    defaults =
        new ImportJobDefaults(
            "localhost:8080",
            "FlinkRunner",
            "{\"key\":\"value\"}",
            "ingestion.jar",
            "stderr",
            "{}");

    flinkJobManager = new FlinkJobManager(flinkCli, config, flinkRestApi, defaults);
  }

  @Test
  public void shouldPassCorrectArgumentForSubmittingJob() {
    FlinkJobList response = new FlinkJobList();
    response.setJobs(Collections.singletonList(new FlinkJob("1234", "job1", "RUNNING")));
    when(flinkRestApi.getJobsOverview()).thenReturn(response);

    ImportSpec importSpec = ImportSpec.newBuilder().setType("file").build();
    String jobName = "importjob";
    flinkJobManager.submitJob(importSpec, jobName);
    String[] expected =
        new String[] {
          "run",
          "-d",
          "-m",
          config.getMasterUrl(),
          defaults.getExecutable(),
          "--jobName=" + jobName,
          "--runner=FlinkRunner",
          "--importSpecBase64=CgRmaWxl",
          "--coreApiUri=" + defaults.getCoreApiUri(),
          "--errorsStoreType=" + defaults.getErrorsStoreType(),
          "--errorsStoreOptions=" + defaults.getErrorsStoreOptions(),
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

    ImportSpec importSpec = ImportSpec.newBuilder().setType("file").build();
    String jobId = flinkJobManager.submitJob(importSpec, jobName);

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
