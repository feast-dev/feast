/*
 * Copyright 2018 The Feast Authors
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
 *
 */

package feast.core.job.dataflow;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.api.services.dataflow.Dataflow;
import feast.core.config.ImportJobDefaults;
import feast.core.exception.JobExecutionException;
import feast.ingestion.options.ImportJobPipelineOptions;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;

public class DataflowJobManagerTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private Dataflow dataflow;


  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private ImportJobDefaults defaults;
  private DataflowJobManager dfJobManager;
  private Path workspace;

  @Before
  public void setUp() throws IOException {
    initMocks(this);
    workspace = Paths.get(tempFolder.newFolder().toString());
    defaults =
        ImportJobDefaults.builder()
            .runner("DataflowRunner")
            .importJobOptions("{\"region\":\"region\"}")
            .workspace(workspace.toString()).build();
    dfJobManager = new DataflowJobManager(dataflow, "project", "location", defaults);
  }

  @Test
  public void shouldSubmitDataflowJobAndReturnId() throws IOException, URISyntaxException {
    dfJobManager = Mockito.spy(dfJobManager);

    ImportJobPipelineOptions expectedPipelineOptions = PipelineOptionsFactory.fromArgs("")
        .as(ImportJobPipelineOptions.class);
    expectedPipelineOptions.setRunner(DataflowRunner.class);
    expectedPipelineOptions.setProject("project");
    expectedPipelineOptions.setRegion("region");
    expectedPipelineOptions.setUpdate(false);
    expectedPipelineOptions.setAppName("DataflowJobManager");
    expectedPipelineOptions.setWorkspace(workspace.toUri().toString());
    expectedPipelineOptions
        .setImportJobSpecUri(workspace.resolve("importJobSpecs.yaml").toUri().toString());

    String expectedJobId = "feast-job-0";
    ArgumentCaptor<ImportJobPipelineOptions> captor = ArgumentCaptor
        .forClass(ImportJobPipelineOptions.class);

    DataflowPipelineJob mockPipelineResult = Mockito.mock(DataflowPipelineJob.class);
    when(mockPipelineResult.getState()).thenReturn(State.RUNNING);
    when(mockPipelineResult.getJobId()).thenReturn(expectedJobId);

    doReturn(mockPipelineResult).when(dfJobManager).runPipeline(any());
    String jobId = dfJobManager.startJob("job", workspace);

    verify(dfJobManager, times(1)).runPipeline(captor.capture());
    ImportJobPipelineOptions actualPipelineOptions = captor.getValue();
    expectedPipelineOptions.setOptionsId(actualPipelineOptions.getOptionsId()); // avoid comparing this value

    assertThat(actualPipelineOptions.toString(),
        equalTo(expectedPipelineOptions.toString()));
    assertThat(jobId, equalTo(expectedJobId));
  }

  @Test
  public void shouldThrowExceptionWhenJobStateTerminal() throws IOException, URISyntaxException {
    dfJobManager = Mockito.spy(dfJobManager);

    DataflowPipelineJob mockPipelineResult = Mockito.mock(DataflowPipelineJob.class);
    when(mockPipelineResult.getState()).thenReturn(State.FAILED);

    doReturn(mockPipelineResult).when(dfJobManager).runPipeline(any());

    expectedException.expect(JobExecutionException.class);
    dfJobManager.startJob("job", workspace);
  }
}
