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
package feast.jobcontroller.runner.dataflow;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Environment;
import com.google.api.services.dataflow.model.ListJobsResponse;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Printer;
import feast.ingestion.options.ImportOptions;
import feast.jobcontroller.config.FeastProperties;
import feast.jobcontroller.exception.JobExecutionException;
import feast.jobcontroller.model.Job;
import feast.proto.core.IngestionJobProto;
import feast.proto.core.RunnerProto.DataflowRunnerConfigOptions;
import feast.proto.core.RunnerProto.DataflowRunnerConfigOptions.Builder;
import feast.proto.core.SourceProto;
import feast.proto.core.SourceProto.KafkaSourceConfig;
import feast.proto.core.SourceProto.SourceType;
import feast.proto.core.StoreProto;
import feast.proto.core.StoreProto.Store.RedisConfig;
import feast.proto.core.StoreProto.Store.StoreType;
import feast.proto.core.StoreProto.Store.Subscription;
import java.io.IOException;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class DataflowJobManagerTest {

  private Dataflow dataflow;

  private DataflowRunnerConfigOptions defaults;
  private IngestionJobProto.SpecsStreamingUpdateConfig specsStreamingUpdateConfig;
  private DataflowJobManager dfJobManager;

  private StoreProto.Store store;
  private SourceProto.Source source;

  @BeforeEach
  public void setUp() {
    Builder optionsBuilder = DataflowRunnerConfigOptions.newBuilder();
    optionsBuilder.setProject("project");
    optionsBuilder.setRegion("region");
    optionsBuilder.setWorkerZone("zone");
    optionsBuilder.setTempLocation("tempLocation");
    optionsBuilder.setNetwork("network");
    optionsBuilder.setSubnetwork("subnetwork");
    optionsBuilder.putLabels("orchestrator", "feast");
    defaults = optionsBuilder.build();
    FeastProperties.MetricsProperties metricsProperties = new FeastProperties.MetricsProperties();
    metricsProperties.setEnabled(false);

    dataflow = mock(Dataflow.class, RETURNS_DEEP_STUBS);

    specsStreamingUpdateConfig =
        IngestionJobProto.SpecsStreamingUpdateConfig.newBuilder()
            .setSource(
                KafkaSourceConfig.newBuilder()
                    .setTopic("specs_topic")
                    .setBootstrapServers("servers:9092")
                    .build())
            .build();

    store =
        StoreProto.Store.newBuilder()
            .setName("SERVING")
            .setType(StoreType.REDIS)
            .setRedisConfig(RedisConfig.newBuilder().setHost("localhost").setPort(6379).build())
            .addSubscriptions(Subscription.newBuilder().setProject("*").setName("*").build())
            .build();

    source =
        SourceProto.Source.newBuilder()
            .setType(SourceType.KAFKA)
            .setKafkaSourceConfig(
                KafkaSourceConfig.newBuilder()
                    .setTopic("topic")
                    .setBootstrapServers("servers:9092")
                    .build())
            .build();

    dfJobManager =
        new DataflowJobManager(
            defaults,
            metricsProperties,
            specsStreamingUpdateConfig,
            ImmutableMap.of("application", "feast"),
            dataflow);
    dfJobManager = spy(dfJobManager);
  }

  @Test
  public void shouldStartJobWithCorrectPipelineOptions() throws IOException {
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
    expectedPipelineOptions.setLabels(defaults.getLabelsMap());
    expectedPipelineOptions.setJobName(jobName);
    expectedPipelineOptions.setStoresJson(Lists.newArrayList(printer.print(store)));
    expectedPipelineOptions.setSourceJson(printer.print(source));

    ArgumentCaptor<ImportOptions> captor = ArgumentCaptor.forClass(ImportOptions.class);

    DataflowPipelineJob mockPipelineResult = Mockito.mock(DataflowPipelineJob.class);
    when(mockPipelineResult.getState()).thenReturn(State.RUNNING);
    when(mockPipelineResult.getJobId()).thenReturn(expectedExtJobId);

    doReturn(mockPipelineResult).when(dfJobManager).runPipeline(any());

    Job job =
        Job.builder()
            .setId(jobName)
            .setSource(source)
            .setStores(ImmutableMap.of(store.getName(), store))
            .build();
    Job actual = dfJobManager.startJob(job);

    verify(dfJobManager, times(1)).runPipeline(captor.capture());
    ImportOptions actualPipelineOptions = captor.getValue();

    expectedPipelineOptions.setOptionsId(
        actualPipelineOptions.getOptionsId()); // avoid comparing this value

    // We only check that we are calling getFilesToStage() manually, because the automatic approach
    // throws an error: https://github.com/feast-dev/feast/pull/291 i.e. do not check for the actual
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
    assertThat(actual.getExtId(), equalTo(expectedExtJobId));
  }

  @Test
  public void shouldThrowExceptionWhenJobStateTerminal() throws IOException {
    dfJobManager = Mockito.spy(dfJobManager);

    DataflowPipelineJob mockPipelineResult = Mockito.mock(DataflowPipelineJob.class);
    when(mockPipelineResult.getState()).thenReturn(State.FAILED);

    doReturn(mockPipelineResult).when(dfJobManager).runPipeline(any());

    Job job =
        Job.builder()
            .setId("job")
            .setSource(source)
            .setStores(ImmutableMap.of(store.getName(), store))
            .build();
    assertThrows(JobExecutionException.class, () -> dfJobManager.startJob(job));
  }

  @Test
  @SneakyThrows
  public void shouldRetrieveRunningJobsFromDataflow() {
    when(dataflow
            .projects()
            .locations()
            .jobs()
            .list("project", "region")
            .setFilter("ACTIVE")
            .execute())
        .thenReturn(
            new ListJobsResponse()
                .setJobs(
                    ImmutableList.of(
                        new com.google.api.services.dataflow.model.Job().setId("job-1"),
                        new com.google.api.services.dataflow.model.Job().setId("job-2"))));

    // Job doesn't have required labels, should be skipped
    when(dataflow
            .projects()
            .locations()
            .jobs()
            .get("project", "region", "job-1")
            .setView("JOB_VIEW_ALL")
            .execute())
        .thenReturn(new com.google.api.services.dataflow.model.Job());

    Printer jsonPrinter = JsonFormat.printer();

    LocalDateTime created = DateTime.now().toLocalDateTime();

    when(dataflow
            .projects()
            .locations()
            .jobs()
            .get("project", "region", "job-2")
            .setView("JOB_VIEW_ALL")
            .execute())
        .thenReturn(
            new com.google.api.services.dataflow.model.Job()
                .setLabels(ImmutableMap.of("application", "feast"))
                .setId("job-2")
                .setCreateTime(created.toString())
                .setEnvironment(
                    new Environment()
                        .setSdkPipelineOptions(
                            ImmutableMap.of(
                                "options",
                                ImmutableMap.of(
                                    "jobName", "kafka-to-redis",
                                    "sourceJson", jsonPrinter.print(source),
                                    "storesJson", ImmutableList.of(jsonPrinter.print(store)))))));

    List<Job> jobs = dfJobManager.listRunningJobs();

    assertThat(jobs, hasSize(1));
    assertThat(
        jobs,
        hasItem(
            allOf(
                hasProperty("id", equalTo("kafka-to-redis")),
                hasProperty("source", equalTo(source)),
                hasProperty("stores", hasValue(store)),
                hasProperty("extId", equalTo("job-2")),
                hasProperty("created", equalTo(created.toDate())),
                hasProperty("lastUpdated", equalTo(created.toDate())),
                hasProperty("labels", hasEntry("application", "feast")))));
  }

  @Test
  @SneakyThrows
  public void shouldHandleNullResponseFromDataflow() {
    when(dataflow
            .projects()
            .locations()
            .jobs()
            .list("project", "region")
            .setFilter("ACTIVE")
            .execute()
            .getJobs())
        .thenReturn(null);

    assertThat(dfJobManager.listRunningJobs(), hasSize(0));
  }

  @Test
  @SneakyThrows
  public void shouldRetrieveRunningJobsWithoutLabels() {
    when(dataflow
            .projects()
            .locations()
            .jobs()
            .list("project", "region")
            .setFilter("ACTIVE")
            .execute())
        .thenReturn(
            new ListJobsResponse()
                .setJobs(
                    ImmutableList.of(
                        new com.google.api.services.dataflow.model.Job().setId("job-1"))));

    Printer jsonPrinter = JsonFormat.printer();

    // job with no labels
    when(dataflow
            .projects()
            .locations()
            .jobs()
            .get("project", "region", "job-1")
            .setView("JOB_VIEW_ALL")
            .execute())
        .thenReturn(
            new com.google.api.services.dataflow.model.Job()
                .setId("job-1")
                .setEnvironment(
                    new Environment()
                        .setSdkPipelineOptions(
                            ImmutableMap.of(
                                "options",
                                ImmutableMap.of(
                                    "jobName", "kafka-to-redis",
                                    "sourceJson", jsonPrinter.print(source),
                                    "storesJson", ImmutableList.of(jsonPrinter.print(store)))))));

    FeastProperties.MetricsProperties metricsProperties = new FeastProperties.MetricsProperties();
    metricsProperties.setEnabled(false);

    dfJobManager =
        new DataflowJobManager(
            defaults, metricsProperties, specsStreamingUpdateConfig, ImmutableMap.of(), dataflow);

    List<Job> jobs = dfJobManager.listRunningJobs();
    assertThat(jobs, hasItem(hasProperty("id", equalTo("kafka-to-redis"))));
  }
}
