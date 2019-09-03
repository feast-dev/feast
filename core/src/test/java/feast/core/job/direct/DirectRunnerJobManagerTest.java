package feast.core.job.direct;

public class DirectRunnerJobManagerTest {
//  @Rule
//  public final ExpectedException expectedException = ExpectedException.none();
//
//  @Rule
//  public TemporaryFolder tempFolder = new TemporaryFolder();
//
//  @Mock
//  private DirectJobRegistry directJobRegistry;
//
//  private ImportJobDefaults defaults;
//  private DirectRunnerJobManager drJobManager;
//  private Path workspace;
//
//  @Before
//  public void setUp() throws IOException {
//    initMocks(this);
//    workspace = Paths.get(tempFolder.newFolder().toString());
//    defaults =
//        ImportJobDefaults.builder()
//            .runner("DataflowRunner")
//            .importJobOptions("{\"coalesceRowsEnabled\":\"true\"}")
//            .workspace(workspace.toString()).build();
//    drJobManager = new DirectRunnerJobManager(defaults, directJobRegistry);
//  }
//
//  @Test
//  public void shouldStartDirectJobAndRegisterPipelineResult() throws IOException, URISyntaxException {
//    drJobManager = Mockito.spy(drJobManager);
//
//    ImportJobPipelineOptions expectedPipelineOptions = PipelineOptionsFactory.fromArgs("")
//        .as(ImportJobPipelineOptions.class);
//    expectedPipelineOptions.setAppName("DirectRunnerJobManager");
//    expectedPipelineOptions.setCoalesceRowsEnabled(true);
//    expectedPipelineOptions.setRunner(DirectRunner.class);
//    expectedPipelineOptions.setWorkspace(workspace.toUri().toString());
//    expectedPipelineOptions.setImportJobSpecUri(workspace.resolve("importJobSpecs.yaml").toUri().toString());
//    expectedPipelineOptions.setBlockOnRun(false);
//
//    String expectedJobId = "feast-job-0";
//    ArgumentCaptor<ImportJobPipelineOptions> pipelineOptionsCaptor = ArgumentCaptor
//        .forClass(ImportJobPipelineOptions.class);
//    ArgumentCaptor<DirectJob> directJobCaptor = ArgumentCaptor
//        .forClass(DirectJob.class);
//
//    PipelineResult mockPipelineResult = Mockito.mock(PipelineResult.class);
//    doReturn(mockPipelineResult).when(drJobManager).runPipeline(any());
//
//    String jobId = drJobManager.startJob(expectedJobId, workspace);
//    Mockito.verify(drJobManager, times(1)).runPipeline(pipelineOptionsCaptor.capture());
//    Mockito.verify(directJobRegistry, times(1)).add(directJobCaptor.capture());
//
//    ImportJobPipelineOptions actualPipelineOptions = pipelineOptionsCaptor.getValue();
//    DirectJob jobStarted = directJobCaptor.getValue();
//    expectedPipelineOptions.setOptionsId(actualPipelineOptions.getOptionsId()); // avoid comparing this value
//
//    assertThat(actualPipelineOptions.toString(),
//        equalTo(expectedPipelineOptions.toString()));
//    assertThat(jobStarted.getPipelineResult(), equalTo(mockPipelineResult));
//    assertThat(jobStarted.getJobId(), equalTo(expectedJobId));
//    assertThat(jobId, equalTo(expectedJobId));
//  }
//
//  @Test
//  public void shouldAbortJobThenRemoveFromRegistry() throws IOException {
//    DirectJob job = Mockito.mock(DirectJob.class);
//    when(directJobRegistry.get("job")).thenReturn(job);
//    drJobManager.abortJob("job");
//    verify(job, times(1)).abort();
//    verify(directJobRegistry, times(1)).remove("job");
//  }
}