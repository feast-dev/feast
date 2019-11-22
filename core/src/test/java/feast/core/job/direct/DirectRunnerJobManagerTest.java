package feast.core.job.direct;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.collect.Lists;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Printer;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.StoreProto;
import feast.core.StoreProto.Store.RedisConfig;
import feast.core.StoreProto.Store.StoreType;
import feast.core.config.FeastProperties.MetricsProperties;
import feast.ingestion.options.ImportOptions;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;

public class DirectRunnerJobManagerTest {
  @Rule public final ExpectedException expectedException = ExpectedException.none();

  @Mock private DirectJobRegistry directJobRegistry;

  private DirectRunnerJobManager drJobManager;
  private Map<String, String> defaults;

  @Before
  public void setUp() {
    initMocks(this);
    defaults = new HashMap<>();
    MetricsProperties metricsProperties = new MetricsProperties();
    metricsProperties.setEnabled(false);

    drJobManager = new DirectRunnerJobManager(defaults, directJobRegistry, metricsProperties);
    drJobManager = Mockito.spy(drJobManager);
  }

  @Test
  public void shouldStartDirectJobAndRegisterPipelineResult() throws IOException {
    StoreProto.Store store =
        StoreProto.Store.newBuilder()
            .setName("SERVING")
            .setType(StoreType.REDIS)
            .setRedisConfig(RedisConfig.newBuilder().setHost("localhost").setPort(6379).build())
            .build();

    FeatureSetSpec featureSetSpec =
        FeatureSetSpec.newBuilder().setName("featureSet").setVersion(1).build();

    Printer printer = JsonFormat.printer();

    ImportOptions expectedPipelineOptions =
        PipelineOptionsFactory.fromArgs("").as(ImportOptions.class);
    expectedPipelineOptions.setAppName("DirectRunnerJobManager");
    expectedPipelineOptions.setRunner(DirectRunner.class);
    expectedPipelineOptions.setBlockOnRun(false);
    expectedPipelineOptions.setProject("");
    expectedPipelineOptions.setStoreJson(Lists.newArrayList(printer.print(store)));
    expectedPipelineOptions.setProject("");
    expectedPipelineOptions.setFeatureSetSpecJson(
        Lists.newArrayList(printer.print(featureSetSpec)));

    String expectedJobId = "feast-job-0";
    ArgumentCaptor<ImportOptions> pipelineOptionsCaptor =
        ArgumentCaptor.forClass(ImportOptions.class);
    ArgumentCaptor<DirectJob> directJobCaptor = ArgumentCaptor.forClass(DirectJob.class);

    PipelineResult mockPipelineResult = Mockito.mock(PipelineResult.class);
    doReturn(mockPipelineResult).when(drJobManager).runPipeline(any());

    String jobId = drJobManager.startJob(expectedJobId, Lists.newArrayList(featureSetSpec), store);
    verify(drJobManager, times(1)).runPipeline(pipelineOptionsCaptor.capture());
    verify(directJobRegistry, times(1)).add(directJobCaptor.capture());

    ImportOptions actualPipelineOptions = pipelineOptionsCaptor.getValue();
    DirectJob jobStarted = directJobCaptor.getValue();
    expectedPipelineOptions.setOptionsId(
        actualPipelineOptions.getOptionsId()); // avoid comparing this value

    assertThat(actualPipelineOptions.toString(), equalTo(expectedPipelineOptions.toString()));
    assertThat(jobStarted.getPipelineResult(), equalTo(mockPipelineResult));
    assertThat(jobStarted.getJobId(), equalTo(expectedJobId));
    assertThat(jobId, equalTo(expectedJobId));
  }

  @Test
  public void shouldAbortJobThenRemoveFromRegistry() throws IOException {
    DirectJob job = Mockito.mock(DirectJob.class);
    when(directJobRegistry.get("job")).thenReturn(job);
    drJobManager.abortJob("job");
    verify(job, times(1)).abort();
    verify(directJobRegistry, times(1)).remove("job");
  }
}
