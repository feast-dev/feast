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
package feast.core.job.dataflow;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.api.services.dataflow.Dataflow;
import com.google.common.collect.Lists;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Printer;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.StoreProto;
import feast.core.StoreProto.Store.RedisConfig;
import feast.core.StoreProto.Store.StoreType;
import feast.core.config.FeastProperties.MetricsProperties;
import feast.core.exception.JobExecutionException;
import feast.ingestion.options.ImportOptions;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;

public class DataflowJobManagerTest {

  @Rule public final ExpectedException expectedException = ExpectedException.none();

  @Mock private Dataflow dataflow;

  private Map<String, String> defaults;
  private DataflowJobManager dfJobManager;

  @Before
  public void setUp() {
    initMocks(this);
    defaults = new HashMap<>();
    defaults.put("project", "project");
    defaults.put("region", "region");
    MetricsProperties metricsProperties = new MetricsProperties();
    metricsProperties.setEnabled(false);
    dfJobManager = new DataflowJobManager(dataflow, defaults, metricsProperties);
    dfJobManager = spy(dfJobManager);
  }

  @Test
  public void shouldStartJobWithCorrectPipelineOptions() throws IOException {
    StoreProto.Store store =
        StoreProto.Store.newBuilder()
            .setName("SERVING")
            .setType(StoreType.REDIS)
            .setRedisConfig(RedisConfig.newBuilder().setHost("localhost").setPort(6379).build())
            .build();

    FeatureSetSpec featureSetSpec =
        FeatureSetSpec.newBuilder().setName("featureSet").setVersion(1).build();

    Printer printer = JsonFormat.printer();
    String expectedExtJobId = "feast-job-0";
    String jobName = "job";

    ImportOptions expectedPipelineOptions =
        PipelineOptionsFactory.fromArgs("").as(ImportOptions.class);
    expectedPipelineOptions.setRunner(DataflowRunner.class);
    expectedPipelineOptions.setProject("project");
    expectedPipelineOptions.setRegion("region");
    expectedPipelineOptions.setUpdate(false);
    expectedPipelineOptions.setAppName("DataflowJobManager");
    expectedPipelineOptions.setJobName(jobName);
    expectedPipelineOptions.setStoreJson(Lists.newArrayList(printer.print(store)));
    expectedPipelineOptions.setFeatureSetSpecJson(
        Lists.newArrayList(printer.print(featureSetSpec)));

    ArgumentCaptor<ImportOptions> captor = ArgumentCaptor.forClass(ImportOptions.class);

    DataflowPipelineJob mockPipelineResult = Mockito.mock(DataflowPipelineJob.class);
    when(mockPipelineResult.getState()).thenReturn(State.RUNNING);
    when(mockPipelineResult.getJobId()).thenReturn(expectedExtJobId);

    doReturn(mockPipelineResult).when(dfJobManager).runPipeline(any());
    String jobId = dfJobManager.startJob(jobName, Lists.newArrayList(featureSetSpec), store);

    verify(dfJobManager, times(1)).runPipeline(captor.capture());
    ImportOptions actualPipelineOptions = captor.getValue();

    expectedPipelineOptions.setOptionsId(
        actualPipelineOptions.getOptionsId()); // avoid comparing this value

    // We only check that we are calling getFilesToStage() manually, because the automatic approach
    // throws an error: https://github.com/gojek/feast/pull/291 i.e. do not check for the actual
    // files that are staged
    assertThat(
        "filesToStage in pipelineOptions should not be null, job manager should set it.",
        actualPipelineOptions.getFilesToStage() != null);
    assertThat(
        "filesToStage in pipelineOptions should contain at least 1 item",
        actualPipelineOptions.getFilesToStage().size() > 0);
    // Assume the files that are staged are correct
    expectedPipelineOptions.setFilesToStage(actualPipelineOptions.getFilesToStage());

    assertThat(actualPipelineOptions.toString(), equalTo(expectedPipelineOptions.toString()));
    assertThat(jobId, equalTo(expectedExtJobId));
  }

  @Test
  public void shouldThrowExceptionWhenJobStateTerminal() throws IOException {
    StoreProto.Store store =
        StoreProto.Store.newBuilder()
            .setName("SERVING")
            .setType(StoreType.REDIS)
            .setRedisConfig(RedisConfig.newBuilder().setHost("localhost").setPort(6379).build())
            .build();

    FeatureSetSpec featureSetSpec =
        FeatureSetSpec.newBuilder().setName("featureSet").setVersion(1).build();

    dfJobManager = Mockito.spy(dfJobManager);

    DataflowPipelineJob mockPipelineResult = Mockito.mock(DataflowPipelineJob.class);
    when(mockPipelineResult.getState()).thenReturn(State.FAILED);

    doReturn(mockPipelineResult).when(dfJobManager).runPipeline(any());

    expectedException.expect(JobExecutionException.class);
    dfJobManager.startJob("job", Lists.newArrayList(featureSetSpec), store);
  }
}
