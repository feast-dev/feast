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
package feast.core.config;

import com.google.gson.Gson;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import feast.core.config.FeastProperties.JobProperties;
import feast.core.dao.JobRepository;
import feast.core.job.ConsolidatedJobStrategy;
import feast.core.job.JobGroupingStrategy;
import feast.core.job.JobManager;
import feast.core.job.JobPerStoreStrategy;
import feast.core.job.dataflow.DataflowJobManager;
import feast.core.job.direct.DirectJobRegistry;
import feast.core.job.direct.DirectRunnerJobManager;
import feast.proto.core.IngestionJobProto;
import feast.proto.core.RunnerProto.DataflowRunnerConfigOptions;
import feast.proto.core.RunnerProto.DirectRunnerConfigOptions;
import feast.proto.core.SourceProto;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Beans for job management */
@Slf4j
@Configuration
public class JobConfig {
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
    Boolean shouldConsolidateJobs = feastProperties.getJobs().getConsolidateJobsPerSource();
    if (shouldConsolidateJobs) {
      return new ConsolidatedJobStrategy(jobRepository);
    } else {
      return new JobPerStoreStrategy(jobRepository);
    }
  }

  /**
   * Get a JobManager according to the runner type and Dataflow configuration.
   *
   * @param feastProperties feast config properties
   */
  @Bean
  @Autowired
  public JobManager getJobManager(
      FeastProperties feastProperties,
      IngestionJobProto.SpecsStreamingUpdateConfig specsStreamingUpdateConfig)
      throws InvalidProtocolBufferException {

    JobProperties jobProperties = feastProperties.getJobs();
    FeastProperties.JobProperties.Runner runner = jobProperties.getActiveRunner();
    Map<String, Object> runnerConfigOptions = runner.getOptions();
    String configJson = gson.toJson(runnerConfigOptions);

    FeastProperties.MetricsProperties metrics = jobProperties.getMetrics();

    switch (runner.getType()) {
      case DATAFLOW:
        DataflowRunnerConfigOptions.Builder dataflowRunnerConfigOptions =
            DataflowRunnerConfigOptions.newBuilder();
        JsonFormat.parser().merge(configJson, dataflowRunnerConfigOptions);
        return new DataflowJobManager(
            dataflowRunnerConfigOptions.build(), metrics, specsStreamingUpdateConfig);
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
}
