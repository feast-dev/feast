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
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.api.services.dataflow.Dataflow;
import com.google.common.collect.Lists;
import com.google.protobuf.Duration;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Printer;
import feast.core.FeatureSetProto;
import feast.core.FeatureSetProto.FeatureSetMeta;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.SourceProto;
import feast.core.SourceProto.KafkaSourceConfig;
import feast.core.SourceProto.SourceType;
import feast.core.StoreProto;
import feast.core.StoreProto.Store.RedisConfig;
import feast.core.StoreProto.Store.StoreType;
import feast.core.StoreProto.Store.Subscription;
import feast.core.config.FeastProperties.MetricsProperties;
import feast.core.exception.JobExecutionException;
import feast.core.job.Runner;
import feast.core.job.option.FeatureSetJsonByteConverter;
import feast.core.model.*;
import feast.ingestion.options.BZip2Compressor;
import feast.ingestion.options.ImportOptions;
import feast.ingestion.options.OptionCompressor;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
    dfJobManager = new DataflowJobManager(defaults, metricsProperties);
    dfJobManager = spy(dfJobManager);
  }

  @Test
  public void shouldStartJobWithCorrectPipelineOptions() throws IOException {
    StoreProto.Store store =
        StoreProto.Store.newBuilder()
            .setName("SERVING")
            .setType(StoreType.REDIS)
            .setRedisConfig(RedisConfig.newBuilder().setHost("localhost").setPort(6379).build())
            .addSubscriptions(
                Subscription.newBuilder().setProject("*").setName("*").setVersion("*").build())
            .build();

    SourceProto.Source source =
        SourceProto.Source.newBuilder()
            .setType(SourceType.KAFKA)
            .setKafkaSourceConfig(
                KafkaSourceConfig.newBuilder()
                    .setTopic("topic")
                    .setBootstrapServers("servers:9092")
                    .build())
            .build();

    FeatureSetProto.FeatureSet featureSet =
        FeatureSetProto.FeatureSet.newBuilder()
            .setMeta(FeatureSetMeta.newBuilder())
            .setSpec(
                FeatureSetSpec.newBuilder()
                    .setSource(source)
                    .setName("featureSet")
                    .setVersion(1)
                    .setMaxAge(Duration.newBuilder().build()))
            .build();

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

    OptionCompressor<List<FeatureSetProto.FeatureSet>> featureSetJsonCompressor =
        new BZip2Compressor<>(new FeatureSetJsonByteConverter());
    expectedPipelineOptions.setFeatureSetJson(
        featureSetJsonCompressor.compress(Collections.singletonList(featureSet)));

    ArgumentCaptor<ImportOptions> captor = ArgumentCaptor.forClass(ImportOptions.class);

    DataflowPipelineJob mockPipelineResult = Mockito.mock(DataflowPipelineJob.class);
    when(mockPipelineResult.getState()).thenReturn(State.RUNNING);
    when(mockPipelineResult.getJobId()).thenReturn(expectedExtJobId);

    doReturn(mockPipelineResult).when(dfJobManager).runPipeline(any());
    Job job =
        new Job(
            jobName,
            "",
            Runner.DATAFLOW.name(),
            Source.fromProto(source),
            Store.fromProto(store),
            Lists.newArrayList(FeatureSet.fromProto(featureSet)),
            JobStatus.PENDING);
    Job actual = dfJobManager.startJob(job);

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

    assertThat(
        actualPipelineOptions.getFeatureSetJson(),
        equalTo(expectedPipelineOptions.getFeatureSetJson()));
    assertThat(
        actualPipelineOptions.getDeadLetterTableSpec(),
        equalTo(expectedPipelineOptions.getDeadLetterTableSpec()));
    assertThat(
        actualPipelineOptions.getStatsdHost(), equalTo(expectedPipelineOptions.getStatsdHost()));
    assertThat(
        actualPipelineOptions.getMetricsExporterType(),
        equalTo(expectedPipelineOptions.getMetricsExporterType()));
    assertThat(
        actualPipelineOptions.getStoreJson(), equalTo(expectedPipelineOptions.getStoreJson()));
    assertThat(actual.getExtId(), equalTo(expectedExtJobId));
  }

  @Test
  public void shouldThrowExceptionWhenJobStateTerminal() throws IOException {
    StoreProto.Store store =
        StoreProto.Store.newBuilder()
            .setName("SERVING")
            .setType(StoreType.REDIS)
            .setRedisConfig(RedisConfig.newBuilder().setHost("localhost").setPort(6379).build())
            .build();

    SourceProto.Source source =
        SourceProto.Source.newBuilder()
            .setType(SourceType.KAFKA)
            .setKafkaSourceConfig(
                KafkaSourceConfig.newBuilder()
                    .setTopic("topic")
                    .setBootstrapServers("servers:9092")
                    .build())
            .build();

    FeatureSetProto.FeatureSet featureSet =
        FeatureSetProto.FeatureSet.newBuilder()
            .setSpec(
                FeatureSetSpec.newBuilder()
                    .setName("featureSet")
                    .setVersion(1)
                    .setSource(source)
                    .build())
            .build();

    dfJobManager = Mockito.spy(dfJobManager);

    DataflowPipelineJob mockPipelineResult = Mockito.mock(DataflowPipelineJob.class);
    when(mockPipelineResult.getState()).thenReturn(State.FAILED);

    doReturn(mockPipelineResult).when(dfJobManager).runPipeline(any());

    Job job =
        new Job(
            "job",
            "",
            Runner.DATAFLOW.name(),
            Source.fromProto(source),
            Store.fromProto(store),
            Lists.newArrayList(FeatureSet.fromProto(featureSet)),
            JobStatus.PENDING);

    expectedException.expect(JobExecutionException.class);
    dfJobManager.startJob(job);
  }
}
