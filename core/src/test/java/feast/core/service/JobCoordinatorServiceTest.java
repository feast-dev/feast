package feast.core.service;

public class JobCoordinatorServiceTest {

//  @Rule
//  public final ExpectedException exception = ExpectedException.none();
//
//  @Mock
//  JobInfoRepository jobInfoRepository;
//  @Mock
//  JobManager jobManager;
//  @Mock
//  FeatureStream featureStream;
//  private ImportJobDefaults defaults;
//
//  @Before
//  public void setUp() {
//    initMocks(this);
//    defaults = ImportJobDefaults.builder().build();
//  }
//
//  @Test
//  public void shouldCorrectlyCreateImportJobSpecGivenTopicAndSpecs() {
//    JobCoordinatorService jobCoordinatorService = new JobCoordinatorService(jobInfoRepository,
//        jobManager, featureStream, defaults);
//    when(featureStream.getType()).thenReturn("kafka");
//    Map<String, String> kafkaFeatureStreamOptions = new HashMap<>();
//    kafkaFeatureStreamOptions.put("servers", "127.0.0.1:8081");
//    kafkaFeatureStreamOptions.put("discardUnknownFeatures", "true");
//    when(featureStream.getFeatureStreamOptions()).thenReturn(kafkaFeatureStreamOptions);
//    EntitySpec entitySpec = EntitySpec.newBuilder().setName("entity").build();
//    FeatureSpec feature =
//        FeatureSpec.newBuilder()
//            .setId("entity.name")
//            .setName("name")
//            .setOwner("owner")
//            .setDescription("desc")
//            .setEntity("entity")
//            .setUri("uri")
//            .setGroup("testGroup")
//            .setValueType(ValueType.Enum.BYTES)
//            .build();
//    StorageSpec sinkStoreSpec = StorageSpec.newBuilder().setId("SERVING").setType("redis")
//        .putOptions("host", "localhost")
//        .putOptions("port", "1234")
//        .build();
//    StorageSpec errorsStoreSpec = StorageSpec.newBuilder().setId("SERVING").setType("file.json")
//        .putOptions("path", "gs://lalalala")
//        .build();
//    ImportJobSpecs actual = jobCoordinatorService
//        .createImportJobSpecs("feast-entity-features", entitySpec,
//            Arrays.asList(feature), sinkStoreSpec, errorsStoreSpec);
//    ImportJobSpecs expected = ImportJobSpecs.newBuilder()
//        .setJobId(actual.getJobId())
//        .setSourceSpec(SourceSpec.newBuilder().setType(SourceType.KAFKA)
//            .putAllOptions(kafkaFeatureStreamOptions).putOptions("topics", "feast-entity-features")
//            .build())
//        .setEntitySpec(entitySpec)
//        .addAllFeatureSpecs(Arrays.asList(feature))
//        .setSinkStorageSpec(sinkStoreSpec)
//        .setErrorsStorageSpec(errorsStoreSpec)
//        .build();
//    assertThat(actual, equalTo(expected));
//
//    // test the jobId separately
//    assertThat(actual.getJobId(), StringStartsWith.startsWith("feast-entity-features-to-serving-"));
//  }
//
//  @Test
//  public void shouldWriteImportJobSpecsToWorkspaceAndStartJob()
//      throws IOException {
//    File tempDir = Files.createTempDir();
//    tempDir.deleteOnExit();
//    String ws = tempDir.getAbsolutePath();
//
//    Map<String, String> kafkaFeatureStreamOptions = new HashMap<>();
//    kafkaFeatureStreamOptions.put("servers", "127.0.0.1:8081");
//    kafkaFeatureStreamOptions.put("discardUnknownFeatures", "true");
//
//    ImportJobSpecs importJobSpecs = ImportJobSpecs.newBuilder()
//        .setJobId("job1")
//        .setSourceSpec(SourceSpec.newBuilder().setType(SourceType.KAFKA)
//            .putAllOptions(kafkaFeatureStreamOptions).putOptions("topics", "feast-entity-features")
//            .build())
//        .setEntitySpec(EntitySpec.newBuilder().setName("entity").build())
//        .addAllFeatureSpecs(Arrays.asList(FeatureSpec.newBuilder().setId("entity.feature").build()))
//        .setSinkStorageSpec(StorageSpec.newBuilder().setId("sink").build())
//        .setErrorsStorageSpec(StorageSpec.newBuilder().setId("errors").build())
//        .build();
//
//    JobCoordinatorService jobCoordinatorService = new JobCoordinatorService(jobInfoRepository,
//        jobManager, featureStream, defaults);
//
//    defaults.setWorkspace(ws);
//    defaults.setRunner(Runner.DIRECT.getName());
//
//    when(jobManager.startJob("job1", PathUtil.getPath(ws).resolve("job1"))).thenReturn("job1extId");
//
//    JobInfo jobInfo = jobCoordinatorService.startJob(importJobSpecs);
//    JobInfo expected = new JobInfo("job1", "job1extID", Runner.DIRECT.getName(), importJobSpecs,
//        JobStatus.COMPLETED);
//
//    String specsPath = Strings.lenientFormat("%s/%s/%s", ws, "job1", "importJobSpecs.yaml");
//
//    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
//
//    Map<String, Object> writtenSpecYaml = mapper
//        .readValue(Files.toByteArray(new File(specsPath)), Map.class);
//    Gson gson = new Gson();
//    String json = gson.toJson(writtenSpecYaml);
//    Builder writtenSpecBuilder = ImportJobSpecs.newBuilder();
//    JsonFormat.parser().merge(json, writtenSpecBuilder);
//    ImportJobSpecs writtenSpecs = writtenSpecBuilder.build();
//
//    assertEquals(importJobSpecs, writtenSpecs);
//    assertThat(jobInfo, equalTo(expected));
//  }
//
//  @Test
//  public void shouldThrowErrorIfJobIdNotFoundWhenAbortingJob() {
//    when(jobInfoRepository.findById("job1")).thenReturn(Optional.empty());
//    JobCoordinatorService jobCoordinatorService =
//        new JobCoordinatorService(jobInfoRepository,
//            jobManager, featureStream, defaults);
//    exception.expect(RetrievalException.class);
//    exception.expectMessage("Unable to retrieve job with id job1");
//    jobCoordinatorService.abortJob("job1");
//  }
//
//  @Test
//  public void shouldThrowErrorIfJobInTerminalStateWhenAbortingJob() {
//    JobInfo job = new JobInfo();
//    job.setStatus(JobStatus.COMPLETED);
//    when(jobInfoRepository.findById("job1")).thenReturn(Optional.of(job));
//    JobCoordinatorService jobCoordinatorService =
//        new JobCoordinatorService(jobInfoRepository,
//            jobManager, featureStream, defaults);
//    exception.expect(IllegalStateException.class);
//    exception.expectMessage("Unable to stop job already in terminal state");
//    jobCoordinatorService.abortJob("job1");
//  }
//
//  @Test
//  public void shouldUpdateJobAfterAborting() {
//    JobInfo job = new JobInfo();
//    job.setStatus(JobStatus.RUNNING);
//    job.setExtId("extId1");
//    when(jobInfoRepository.findById("job1")).thenReturn(Optional.of(job));
//    JobCoordinatorService jobCoordinatorService =
//        new JobCoordinatorService(jobInfoRepository,
//            jobManager, featureStream, defaults);
//    jobCoordinatorService.abortJob("job1");
//    ArgumentCaptor<JobInfo> jobCapture = ArgumentCaptor.forClass(JobInfo.class);
//    verify(jobInfoRepository).saveAndFlush(jobCapture.capture());
//    assertThat(jobCapture.getValue().getStatus(), equalTo(JobStatus.ABORTING));
//  }
//
//  @Test
//  public void shouldUpdateJobStatusIfExists() {
//    JobInfo jobInfo = new JobInfo();
//    when(jobInfoRepository.findById("jobid")).thenReturn(Optional.of(jobInfo));
//
//    ArgumentCaptor<JobInfo> jobInfoArgumentCaptor = ArgumentCaptor.forClass(JobInfo.class);
//    JobCoordinatorService jobCoordinatorService =
//        new JobCoordinatorService(jobInfoRepository,
//            jobManager, featureStream, defaults);
//    jobCoordinatorService.updateJobStatus("jobid", JobStatus.PENDING);
//
//    verify(jobInfoRepository, times(1)).save(jobInfoArgumentCaptor.capture());
//
//    JobInfo jobInfoUpdated = new JobInfo();
//    jobInfoUpdated.setStatus(JobStatus.PENDING);
//    assertThat(jobInfoArgumentCaptor.getValue(), equalTo(jobInfoUpdated));
//  }
//
//  @Test
//  public void shouldNotUpdateJobIfSchemaHasNotChanged() throws InvalidProtocolBufferException {
//    Map<String, String> kafkaFeatureStreamOptions = new HashMap<>();
//    kafkaFeatureStreamOptions.put("servers", "127.0.0.1:8081");
//    kafkaFeatureStreamOptions.put("discardUnknownFeatures", "true");
//
//    ImportJobSpecs importJobSpecs = ImportJobSpecs.newBuilder()
//        .setJobId("job1")
//        .setSourceSpec(SourceSpec.newBuilder().setType(SourceType.KAFKA)
//            .putAllOptions(kafkaFeatureStreamOptions).putOptions("topics", "feast-entity-features")
//            .build())
//        .setEntitySpec(EntitySpec.newBuilder().setName("entity").build())
//        .addAllFeatureSpecs(Arrays.asList(FeatureSpec.newBuilder().setId("entity.feature").build()))
//        .setSinkStorageSpec(StorageSpec.newBuilder().setId("sink").build())
//        .setErrorsStorageSpec(StorageSpec.newBuilder().setId("errors").build())
//        .build();
//
//    JobInfo jobInfo = new JobInfo("job1", "extJob1", "DirectRunner", importJobSpecs,
//        JobStatus.RUNNING);
//
//    JobCoordinatorService jobCoordinatorService =
//        new JobCoordinatorService(jobInfoRepository,
//            jobManager, featureStream, defaults);
//    jobCoordinatorService.updateJob(jobInfo, importJobSpecs);
//    verify(jobManager, times(0)).updateJob(ArgumentMatchers.any(), ArgumentMatchers.any());
//  }
//
//  @Test
//  public void shouldUpdateJobIfSchemaHasChanged() throws IOException {
//    File tempDir = Files.createTempDir();
//    tempDir.deleteOnExit();
//    String ws = tempDir.getAbsolutePath();
//    defaults.setWorkspace(ws);
//
//    Map<String, String> kafkaFeatureStreamOptions = new HashMap<>();
//    kafkaFeatureStreamOptions.put("servers", "127.0.0.1:8081");
//    kafkaFeatureStreamOptions.put("discardUnknownFeatures", "true");
//
//    ImportJobSpecs oldImportJobSpecs = ImportJobSpecs.newBuilder()
//        .setJobId("job1")
//        .setSourceSpec(SourceSpec.newBuilder().setType(SourceType.KAFKA)
//            .putAllOptions(kafkaFeatureStreamOptions).putOptions("topics", "feast-entity-features")
//            .build())
//        .setEntitySpec(EntitySpec.newBuilder().setName("entity").build())
//        .addAllFeatureSpecs(Arrays.asList(FeatureSpec.newBuilder().setId("entity.feature").build()))
//        .setSinkStorageSpec(StorageSpec.newBuilder().setId("sink").build())
//        .setErrorsStorageSpec(StorageSpec.newBuilder().setId("errors").build())
//        .build();
//
//    ImportJobSpecs newImportJobSpecs = oldImportJobSpecs
//        .toBuilder()
//        .addFeatureSpecs(FeatureSpec
//            .newBuilder().setId("entity.feature2").build())
//        .build();
//
//    JobInfo oldJobInfo = new JobInfo("job1", "extJob1", "DirectRunner", oldImportJobSpecs,
//        JobStatus.RUNNING);
//    JobInfo newJobInfo = new JobInfo("job1", "extJob2", "DirectRunner", newImportJobSpecs,
//        JobStatus.RUNNING);
//
//    when(jobManager.updateJob(oldJobInfo, PathUtil.getPath(ws).resolve("job1"))).thenReturn("extJob2");
//    JobCoordinatorService jobCoordinatorService =
//        new JobCoordinatorService(jobInfoRepository,
//            jobManager, featureStream, defaults);
//    jobCoordinatorService.updateJob(oldJobInfo, newImportJobSpecs);
//
//    verify(jobManager, times(1)).updateJob(oldJobInfo, Paths.get(ws).resolve("job1"));
//    verify(jobInfoRepository, times(1)).save(newJobInfo);
//
//    String specsPath = Strings.lenientFormat("%s/%s/%s", ws, "job1", "importJobSpecs.yaml");
//
//    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
//
//    Map<String, Object> writtenSpecYaml = mapper
//        .readValue(Files.toByteArray(new File(specsPath)), Map.class);
//    Gson gson = new Gson();
//    String json = gson.toJson(writtenSpecYaml);
//    Builder writtenSpecBuilder = ImportJobSpecs.newBuilder();
//    JsonFormat.parser().merge(json, writtenSpecBuilder);
//    ImportJobSpecs writtenSpecs = writtenSpecBuilder.build();
//
//    assertThat(newImportJobSpecs, equalTo(writtenSpecs));
//  }

}