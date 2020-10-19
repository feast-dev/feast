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
package feast.jobcontroller.config;

import com.google.gson.Gson;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import feast.jobcontroller.dao.JobRepository;
import feast.jobcontroller.runner.ConsolidatedJobStrategy;
import feast.jobcontroller.runner.JobGroupingStrategy;
import feast.jobcontroller.runner.JobManager;
import feast.jobcontroller.runner.JobPerStoreStrategy;
import feast.jobcontroller.runner.databricks.DatabricksJobManager;
import feast.jobcontroller.runner.dataflow.DataflowJobManager;
import feast.jobcontroller.runner.direct.DirectJobRegistry;
import feast.jobcontroller.runner.direct.DirectRunnerJobManager;
import feast.proto.core.CoreServiceGrpc;
import feast.proto.core.IngestionJobProto;
import feast.proto.core.RunnerProto.DatabricksRunnerConfigOptions;
import feast.proto.core.RunnerProto.DataflowRunnerConfigOptions;
import feast.proto.core.RunnerProto.DirectRunnerConfigOptions;
import feast.proto.core.SourceProto;
import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.net.http.HttpClient;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Beans for job management */
@Slf4j
@Configuration
public class JobControllerConfig {

  private final Gson gson = new Gson();

  /**
   * Create SpecsStreamingUpdateConfig, which is used to set up communications (bi-directional
   * channel) to send new FeatureSetSpec to IngestionJob and receive acknowledgments.
   *
   * @param feastProperties feast config properties
   */
  @Bean
  public IngestionJobProto.SpecsStreamingUpdateConfig createSpecsStreamingUpdateConfig(
      FeastProperties feastProperties) {
    FeastProperties.StreamProperties streamProperties = feastProperties.getStream();

    return IngestionJobProto.SpecsStreamingUpdateConfig.newBuilder()
        .setSource(
            SourceProto.KafkaSourceConfig.newBuilder()
                .setBootstrapServers(streamProperties.getOptions().getBootstrapServers())
                .setTopic(streamProperties.getSpecsOptions().getSpecsTopic())
                .build())
        .setAck(
            SourceProto.KafkaSourceConfig.newBuilder()
                .setBootstrapServers(streamProperties.getOptions().getBootstrapServers())
                .setTopic(streamProperties.getSpecsOptions().getSpecsAckTopic()))
        .build();
  }

  /**
   * Returns Grouping Strategy which is responsible for how Ingestion would be split across job
   * instances (or how Sources and Stores would be grouped together). Choosing strategy depends on
   * FeastProperties config "feast.jobs.consolidate-jobs-per-source".
   *
   * @param feastProperties feast config properties
   * @param jobRepository repository required by strategy
   * @return JobGroupingStrategy
   */
  @Bean
  public JobGroupingStrategy getJobGroupingStrategy(
      FeastProperties feastProperties, JobRepository jobRepository) {
    Boolean shouldConsolidateJobs =
        feastProperties.getJobs().getController().getConsolidateJobsPerSource();
    FeastProperties.JobProperties jobProperties = feastProperties.getJobs();
    if (shouldConsolidateJobs) {
      return new ConsolidatedJobStrategy(jobRepository, jobProperties);
    } else {
      return new JobPerStoreStrategy(jobRepository, jobProperties);
    }
  }

  /**
   * Get a JobManager according to the runner type and Dataflow configuration.
   *
   * @param feastProperties feast config properties
   */
  @Bean
  @ConditionalOnMissingBean
  public JobManager getJobManager(
      FeastProperties feastProperties,
      IngestionJobProto.SpecsStreamingUpdateConfig specsStreamingUpdateConfig)
      throws InvalidProtocolBufferException {

    FeastProperties.JobProperties jobProperties = feastProperties.getJobs();
    FeastProperties.JobProperties.Runner runner = jobProperties.getActiveRunner();
    Map<String, Object> runnerConfigOptions = runner.getOptions();

    FeastProperties.MetricsProperties metrics = jobProperties.getMetrics();
    String configJson = gson.toJson(runnerConfigOptions);

    switch (runner.getType()) {
      case DATAFLOW:
        DataflowRunnerConfigOptions.Builder dataflowRunnerConfigOptions =
            DataflowRunnerConfigOptions.newBuilder();
        JsonFormat.parser().merge(configJson, dataflowRunnerConfigOptions);
        return DataflowJobManager.of(
            dataflowRunnerConfigOptions.build(),
            metrics,
            specsStreamingUpdateConfig,
            jobProperties.getController().getJobSelector());
      case DATABRICKS:
        DatabricksRunnerConfigOptions.Builder databricksRunnerConfigOptions =
            DatabricksRunnerConfigOptions.newBuilder();
        JsonFormat.parser().merge(configJson, databricksRunnerConfigOptions);
        return new DatabricksJobManager(
            databricksRunnerConfigOptions.build(),
            metrics,
            specsStreamingUpdateConfig,
            HttpClient.newHttpClient());
      case DIRECT:
        DirectRunnerConfigOptions.Builder directRunnerConfigOptions =
            DirectRunnerConfigOptions.newBuilder();
        JsonFormat.parser().merge(configJson, directRunnerConfigOptions);
        return new DirectRunnerJobManager(
            directRunnerConfigOptions.build(),
            new DirectJobRegistry(),
            metrics,
            specsStreamingUpdateConfig);
      default:
        throw new IllegalArgumentException("Unsupported runner: " + runner);
    }
  }

  @Bean
  public CoreServiceGrpc.CoreServiceBlockingStub coreService(
      FeastProperties feastProperties, ObjectProvider<CallCredentials> callCredentials) {
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress(
                feastProperties.getCoreHost(), feastProperties.getCorePort())
            .usePlaintext()
            .build();
    CallCredentials creds = callCredentials.getIfAvailable();

    CoreServiceGrpc.CoreServiceBlockingStub blockingStub;
    if (creds != null) {
      blockingStub = CoreServiceGrpc.newBlockingStub(channel).withCallCredentials(creds);
    } else {
      blockingStub = CoreServiceGrpc.newBlockingStub(channel);
    }
    return blockingStub;
  }
}
