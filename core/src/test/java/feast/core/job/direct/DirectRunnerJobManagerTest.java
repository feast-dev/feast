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
package feast.core.job.direct;

import static feast.core.util.TestUtil.makeFeatureSetJobStatus;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.protobuf.Duration;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Printer;
import feast.core.config.FeastProperties.MetricsProperties;
import feast.core.job.Runner;
import feast.core.model.FeatureSet;
import feast.core.model.Job;
import feast.core.model.JobStatus;
import feast.core.model.Source;
import feast.core.model.Store;
import feast.ingestion.options.ImportOptions;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.IngestionJobProto;
import feast.proto.core.RunnerProto.DirectRunnerConfigOptions;
import feast.proto.core.SourceProto;
import feast.proto.core.SourceProto.KafkaSourceConfig;
import feast.proto.core.SourceProto.SourceType;
import feast.proto.core.StoreProto;
import feast.proto.core.StoreProto.Store.RedisConfig;
import feast.proto.core.StoreProto.Store.StoreType;
import feast.proto.core.StoreProto.Store.Subscription;
import java.io.IOException;
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
  private DirectRunnerConfigOptions defaults;
  private IngestionJobProto.SpecsStreamingUpdateConfig specsStreamingUpdateConfig;

  @Before
  public void setUp() {
    initMocks(this);
    defaults = DirectRunnerConfigOptions.newBuilder().setTargetParallelism(1).build();
    MetricsProperties metricsProperties = new MetricsProperties();
    metricsProperties.setEnabled(false);

    specsStreamingUpdateConfig =
        IngestionJobProto.SpecsStreamingUpdateConfig.newBuilder()
            .setSource(
                KafkaSourceConfig.newBuilder()
                    .setTopic("specs_topic")
                    .setBootstrapServers("servers:9092")
                    .build())
            .build();

    drJobManager =
        new DirectRunnerJobManager(
            defaults, directJobRegistry, metricsProperties, specsStreamingUpdateConfig);
    drJobManager = Mockito.spy(drJobManager);
  }

  @Test
  public void shouldStartDirectJobAndRegisterPipelineResult() throws IOException {
    StoreProto.Store store =
        StoreProto.Store.newBuilder()
            .setName("SERVING")
            .setType(StoreType.REDIS)
            .setRedisConfig(RedisConfig.newBuilder().setHost("localhost").setPort(6379).build())
            .addSubscriptions(Subscription.newBuilder().setProject("*").setName("*").build())
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
                    .setMaxAge(Duration.newBuilder())
                    .setSource(source)
                    .build())
            .build();

    Printer printer = JsonFormat.printer();

    String expectedJobId = "feast-job-0";
    ImportOptions expectedPipelineOptions =
        PipelineOptionsFactory.fromArgs("").as(ImportOptions.class);
    expectedPipelineOptions.setJobName(expectedJobId);
    expectedPipelineOptions.setAppName("DirectRunnerJobManager");
    expectedPipelineOptions.setRunner(DirectRunner.class);
    expectedPipelineOptions.setBlockOnRun(false);
    expectedPipelineOptions.setTargetParallelism(1);
    expectedPipelineOptions.setStoresJson(Lists.newArrayList(printer.print(store)));
    expectedPipelineOptions.setProject("");
    expectedPipelineOptions.setSourceJson(printer.print(source));

    ArgumentCaptor<ImportOptions> pipelineOptionsCaptor =
        ArgumentCaptor.forClass(ImportOptions.class);
    ArgumentCaptor<DirectJob> directJobCaptor = ArgumentCaptor.forClass(DirectJob.class);

    PipelineResult mockPipelineResult = Mockito.mock(PipelineResult.class);
    doReturn(mockPipelineResult).when(drJobManager).runPipeline(any());

    Job job =
        Job.builder()
            .setId(expectedJobId)
            .setExtId("")
            .setRunner(Runner.DIRECT)
            .setSource(Source.fromProto(source))
            .setFeatureSetJobStatuses(makeFeatureSetJobStatus(FeatureSet.fromProto(featureSet)))
            .setStatus(JobStatus.PENDING)
            .build();
    job.setStores(ImmutableSet.of(Store.fromProto(store)));
    Job actual = drJobManager.startJob(job);

    verify(drJobManager, times(1)).runPipeline(pipelineOptionsCaptor.capture());
    verify(directJobRegistry, times(1)).add(directJobCaptor.capture());
    assertThat(actual.getStatus(), equalTo(JobStatus.RUNNING));

    ImportOptions actualPipelineOptions = pipelineOptionsCaptor.getValue();
    DirectJob jobStarted = directJobCaptor.getValue();
    expectedPipelineOptions.setOptionsId(
        actualPipelineOptions.getOptionsId()); // avoid comparing this value

    assertThat(
        actualPipelineOptions.getDeadLetterTableSpec(),
        equalTo(expectedPipelineOptions.getDeadLetterTableSpec()));
    assertThat(
        actualPipelineOptions.getStatsdHost(), equalTo(expectedPipelineOptions.getStatsdHost()));
    assertThat(
        actualPipelineOptions.getMetricsExporterType(),
        equalTo(expectedPipelineOptions.getMetricsExporterType()));
    assertThat(
        actualPipelineOptions.getStoresJson(), equalTo(expectedPipelineOptions.getStoresJson()));
    assertThat(
        actualPipelineOptions.getSourceJson(), equalTo(expectedPipelineOptions.getSourceJson()));
    assertThat(
        actualPipelineOptions.getSpecsStreamingUpdateConfigJson(),
        equalTo(printer.print(specsStreamingUpdateConfig)));

    assertThat(jobStarted.getPipelineResult(), equalTo(mockPipelineResult));
    assertThat(jobStarted.getJobId(), equalTo(expectedJobId));
    assertThat(actual.getExtId(), equalTo(expectedJobId));
  }

  @Test
  public void shouldAbortJobThenRemoveFromRegistry() throws IOException {
    Job job =
        Job.builder()
            .setId("id")
            .setExtId("ext1")
            .setRunner(Runner.DIRECT)
            .setStatus(JobStatus.RUNNING)
            .build();

    DirectJob directJob = Mockito.mock(DirectJob.class);
    when(directJobRegistry.get("ext1")).thenReturn(directJob);
    job = drJobManager.abortJob(job);
    verify(directJob, times(1)).abort();
    verify(directJobRegistry, times(1)).remove("ext1");
    assertThat(job.getStatus(), equalTo(JobStatus.ABORTING));
  }
}
