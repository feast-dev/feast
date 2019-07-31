package feast.core.service;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Strings;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.protobuf.util.JsonFormat;
import feast.core.config.ImportJobDefaults;
import feast.core.dao.JobInfoRepository;
import feast.core.exception.RetrievalException;
import feast.core.job.JobManager;
import feast.core.job.Runner;
import feast.core.model.JobInfo;
import feast.core.model.JobStatus;
import feast.core.stream.FeatureStream;
import feast.core.util.PathUtil;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.ImportJobSpecsProto.ImportJobSpecs;
import feast.specs.ImportJobSpecsProto.ImportJobSpecs.Builder;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.types.ValueProto.ValueType;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.hamcrest.core.StringStartsWith;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

public class JobCoordinatorServiceTest {

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Mock
  JobInfoRepository jobInfoRepository;
  @Mock
  JobManager jobManager;
  @Mock
  FeatureStream featureStream;
  private ImportJobDefaults defaults;

  @Before
  public void setUp() {
    initMocks(this);
    defaults = ImportJobDefaults.builder().build();
  }

  @Test
  public void shouldCorrectlyCreateImportJobSpecGivenTopicAndSpecs() {
    JobCoordinatorService jobCoordinatorService = new JobCoordinatorService(jobInfoRepository,
        jobManager, featureStream, defaults);
    when(featureStream.getType()).thenReturn("kafka");
    Map<String, String> kafkaFeatureStreamOptions = new HashMap<>();
    kafkaFeatureStreamOptions.put("servers", "127.0.0.1:8081");
    kafkaFeatureStreamOptions.put("discardUnknownFeatures", "true");
    when(featureStream.getFeatureStreamOptions()).thenReturn(kafkaFeatureStreamOptions);
    EntitySpec entitySpec = EntitySpec.newBuilder().setName("entity").build();
    FeatureSpec feature =
        FeatureSpec.newBuilder()
            .setId("entity.name")
            .setName("name")
            .setOwner("owner")
            .setDescription("desc")
            .setEntity("entity")
            .setUri("uri")
            .setGroup("testGroup")
            .setValueType(ValueType.Enum.BYTES)
            .build();
    StorageSpec sinkStoreSpec = StorageSpec.newBuilder().setId("SERVING").setType("redis")
        .putOptions("host", "localhost")
        .putOptions("port", "1234")
        .build();
    StorageSpec errorsStoreSpec = StorageSpec.newBuilder().setId("SERVING").setType("file.json")
        .putOptions("path", "gs://lalalala")
        .build();
    ImportJobSpecs actual = jobCoordinatorService
        .createImportJobSpecs("feast-entity-features", entitySpec,
            Arrays.asList(feature), sinkStoreSpec, errorsStoreSpec);
    ImportJobSpecs expected = ImportJobSpecs.newBuilder()
        .setJobId(actual.getJobId())
        .setType("kafka")
        .setEntitySpec(entitySpec)
        .addAllFeatureSpecs(Arrays.asList(feature))
        .setSinkStorageSpec(sinkStoreSpec)
        .setErrorsStorageSpec(errorsStoreSpec)
        .putAllSourceOptions(kafkaFeatureStreamOptions)
        .putSourceOptions("topics", "feast-entity-features")
        .build();
    assertThat(actual, equalTo(expected));

    // test the jobId separately
    assertThat(actual.getJobId(), StringStartsWith.startsWith("feast-entity-features-to-serving-"));
  }

  @Test
  public void shouldWriteImportJobSpecsToWorkspaceAndStartJob()
      throws IOException {
    File tempDir = Files.createTempDir();
    tempDir.deleteOnExit();
    String ws = tempDir.getAbsolutePath();

    Map<String, String> kafkaFeatureStreamOptions = new HashMap<>();
    kafkaFeatureStreamOptions.put("servers", "127.0.0.1:8081");
    kafkaFeatureStreamOptions.put("discardUnknownFeatures", "true");

    ImportJobSpecs importJobSpecs = ImportJobSpecs.newBuilder()
        .setJobId("job1")
        .setType("kafka")
        .setEntitySpec(EntitySpec.newBuilder().setName("entity").build())
        .addAllFeatureSpecs(Arrays.asList(FeatureSpec.newBuilder().setId("entity.feature").build()))
        .setSinkStorageSpec(StorageSpec.newBuilder().setId("sink").build())
        .setErrorsStorageSpec(StorageSpec.newBuilder().setId("errors").build())
        .putAllSourceOptions(kafkaFeatureStreamOptions)
        .putSourceOptions("topics", "feast-entity-features")
        .build();

    JobCoordinatorService jobCoordinatorService = new JobCoordinatorService(jobInfoRepository,
        jobManager, featureStream, defaults);

    defaults.setWorkspace(ws);
    defaults.setRunner(Runner.DIRECT.getName());

    when(jobManager.startJob("job1", PathUtil.getPath(ws).resolve("job1"))).thenReturn("job1extId");

    JobInfo jobInfo = jobCoordinatorService.startJob(importJobSpecs);
    JobInfo expected = new JobInfo("job1", "job1extID", Runner.DIRECT.getName(), importJobSpecs,
        JobStatus.COMPLETED);

    String specsPath = Strings.lenientFormat("%s/%s/%s", ws, "job1", "importJobSpecs.yaml");

    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    Map<String, Object> writtenSpecYaml = mapper
        .readValue(Files.toByteArray(new File(specsPath)), Map.class);
    Gson gson = new Gson();
    String json = gson.toJson(writtenSpecYaml);
    Builder writtenSpecBuilder = ImportJobSpecs.newBuilder();
    JsonFormat.parser().merge(json, writtenSpecBuilder);
    ImportJobSpecs writtenSpecs = writtenSpecBuilder.build();

    assertEquals(importJobSpecs, writtenSpecs);
    assertThat(jobInfo, equalTo(expected));
  }

  @Test
  public void shouldThrowErrorIfJobIdNotFoundWhenAbortingJob() {
    when(jobInfoRepository.findById("job1")).thenReturn(Optional.empty());
    JobCoordinatorService jobCoordinatorService =
        new JobCoordinatorService(jobInfoRepository,
        jobManager, featureStream, defaults);
    exception.expect(RetrievalException.class);
    exception.expectMessage("Unable to retrieve job with id job1");
    jobCoordinatorService.abortJob("job1");
  }

  @Test
  public void shouldThrowErrorIfJobInTerminalStateWhenAbortingJob() {
    JobInfo job = new JobInfo();
    job.setStatus(JobStatus.COMPLETED);
    when(jobInfoRepository.findById("job1")).thenReturn(Optional.of(job));
    JobCoordinatorService jobCoordinatorService =
        new JobCoordinatorService(jobInfoRepository,
        jobManager, featureStream, defaults);
    exception.expect(IllegalStateException.class);
    exception.expectMessage("Unable to stop job already in terminal state");
    jobCoordinatorService.abortJob("job1");
  }

  @Test
  public void shouldUpdateJobAfterAborting() {
    JobInfo job = new JobInfo();
    job.setStatus(JobStatus.RUNNING);
    job.setExtId("extId1");
    when(jobInfoRepository.findById("job1")).thenReturn(Optional.of(job));
    JobCoordinatorService jobCoordinatorService =
        new JobCoordinatorService(jobInfoRepository,
        jobManager, featureStream, defaults);
    jobCoordinatorService.abortJob("job1");
    ArgumentCaptor<JobInfo> jobCapture = ArgumentCaptor.forClass(JobInfo.class);
    verify(jobInfoRepository).saveAndFlush(jobCapture.capture());
    assertThat(jobCapture.getValue().getStatus(), equalTo(JobStatus.ABORTING));
  }

  @Test
  public void shouldUpdateJobStatusIfExists() {
    JobInfo jobInfo = new JobInfo();
    when(jobInfoRepository.findById("jobid")).thenReturn(Optional.of(jobInfo));

    ArgumentCaptor<JobInfo> jobInfoArgumentCaptor = ArgumentCaptor.forClass(JobInfo.class);
    JobCoordinatorService jobExecutionService =
        new JobCoordinatorService(jobInfoRepository,
        jobManager, featureStream, defaults);
    jobExecutionService.updateJobStatus("jobid", JobStatus.PENDING);

    verify(jobInfoRepository, times(1)).save(jobInfoArgumentCaptor.capture());

    JobInfo jobInfoUpdated = new JobInfo();
    jobInfoUpdated.setStatus(JobStatus.PENDING);
    assertThat(jobInfoArgumentCaptor.getValue(), equalTo(jobInfoUpdated));
  }

}