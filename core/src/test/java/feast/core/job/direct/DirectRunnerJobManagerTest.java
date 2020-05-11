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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.collect.Lists;
import com.google.protobuf.Duration;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Printer;
import feast.core.FeatureSetProto;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.SourceProto;
import feast.core.SourceProto.KafkaSourceConfig;
import feast.core.SourceProto.SourceType;
import feast.core.StoreProto;
import feast.core.StoreProto.Store.RedisConfig;
import feast.core.StoreProto.Store.StoreType;
import feast.core.StoreProto.Store.Subscription;
import feast.core.config.FeastProperties.MetricsProperties;
import feast.core.job.Runner;
import feast.core.job.option.FeatureSetJsonByteConverter;
import feast.core.model.FeatureSet;
import feast.core.model.Job;
import feast.core.model.JobStatus;
import feast.core.model.Source;
import feast.core.model.Store;
import feast.ingestion.options.BZip2Compressor;
import feast.ingestion.options.ImportOptions;
import feast.ingestion.options.OptionCompressor;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
    expectedPipelineOptions.setProject("");
    expectedPipelineOptions.setStoreJson(Lists.newArrayList(printer.print(store)));
    expectedPipelineOptions.setProject("");

    OptionCompressor<List<FeatureSetProto.FeatureSet>> featureSetJsonCompressor =
        new BZip2Compressor<>(new FeatureSetJsonByteConverter());
    expectedPipelineOptions.setFeatureSetJson(
        featureSetJsonCompressor.compress(Collections.singletonList(featureSet)));

    ArgumentCaptor<ImportOptions> pipelineOptionsCaptor =
        ArgumentCaptor.forClass(ImportOptions.class);
    ArgumentCaptor<DirectJob> directJobCaptor = ArgumentCaptor.forClass(DirectJob.class);

    PipelineResult mockPipelineResult = Mockito.mock(PipelineResult.class);
    doReturn(mockPipelineResult).when(drJobManager).runPipeline(any());

    Job job =
        new Job(
            expectedJobId,
            "",
            Runner.DIRECT,
            Source.fromProto(source),
            Store.fromProto(store),
            Lists.newArrayList(FeatureSet.fromProto(featureSet)),
            JobStatus.PENDING);
    Job actual = drJobManager.startJob(job);
    verify(drJobManager, times(1)).runPipeline(pipelineOptionsCaptor.capture());
    verify(directJobRegistry, times(1)).add(directJobCaptor.capture());

    ImportOptions actualPipelineOptions = pipelineOptionsCaptor.getValue();
    DirectJob jobStarted = directJobCaptor.getValue();
    expectedPipelineOptions.setOptionsId(
        actualPipelineOptions.getOptionsId()); // avoid comparing this value

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

    assertThat(jobStarted.getPipelineResult(), equalTo(mockPipelineResult));
    assertThat(jobStarted.getJobId(), equalTo(expectedJobId));
    assertThat(actual.getExtId(), equalTo(expectedJobId));
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
