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

package feast.core.config;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.DataflowScopes;
import com.google.common.base.Strings;
import com.timgroup.statsd.StatsDClient;
import feast.core.job.JobManager;
import feast.core.job.JobMonitor;
import feast.core.job.NoopJobMonitor;
import feast.core.job.Runner;
import feast.core.job.StatsdMetricPusher;
import feast.core.job.dataflow.DataflowJobConfig;
import feast.core.job.dataflow.DataflowJobManager;
import feast.core.job.dataflow.DataflowJobMonitor;
import feast.core.job.direct.DirectRunnerJobManager;
import feast.core.job.flink.FlinkJobManager;
import java.io.IOException;
import java.security.GeneralSecurityException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Beans for job management */
@Slf4j
@Configuration
public class JobConfig {

  /**
   * Get configuration for dataflow connection
   *
   * @param projectId
   * @param location
   * @return DataflowJobConfig
   */
  @Bean
  public DataflowJobConfig getDataflowJobConfig(
      @Value("${feast.jobs.dataflow.projectId}") String projectId,
      @Value("${feast.jobs.dataflow.location}") String location) {
    return new DataflowJobConfig(projectId, location);
  }

  /**
   * Get a JobManager according to the runner type and dataflow configuration.
   *
   * @param runnerType runner type: one of [DataflowRunner, DirectRunner, FlinkRunner]
   * @param dfConfig dataflow job configuration
   * @return JobManager
   */
  @Bean
  public JobManager getJobManager(
      @Value("${feast.jobs.runner}") String runnerType,
      DataflowJobConfig dfConfig,
      ImportJobDefaults defaults) {

    Runner runner = Runner.fromString(runnerType);

    switch (runner) {
      case DATAFLOW:
        if (Strings.isNullOrEmpty(dfConfig.getLocation())
            || Strings.isNullOrEmpty(dfConfig.getProjectId())) {
          log.error("Project and location of the Dataflow runner is not configured");
          throw new IllegalStateException(
              "Project and location of Dataflow runner must be specified for jobs to be run on Dataflow runner.");
        }
        try {
          GoogleCredential credential =
              GoogleCredential.getApplicationDefault().createScoped(DataflowScopes.all());
          Dataflow dataflow =
              new Dataflow(
                  GoogleNetHttpTransport.newTrustedTransport(),
                  JacksonFactory.getDefaultInstance(),
                  credential);

          return new DataflowJobManager(
              dataflow, dfConfig.getProjectId(), dfConfig.getLocation(), defaults);
        } catch (IOException e) {
          throw new IllegalStateException(
              "Unable to find credential required for Dataflow monitoring API", e);
        } catch (GeneralSecurityException e) {
          throw new IllegalStateException("Security exception while connecting to Dataflow API", e);
        } catch (Exception e) {
          throw new IllegalStateException("Unable to initialize DataflowJobManager", e);
        }

      case FLINK:
        return new FlinkJobManager();

      case DIRECT:
        return new DirectRunnerJobManager();
      default:
        throw new IllegalArgumentException("Unsupported runner: " + runnerType);
    }
  }

  /**
   * Get a Job Monitor given the runner type and dataflow configuration.
   *
   * @param runnerType runner type: one of [DataflowRunner, DirectRunner, FlinkRunner]
   * @param dfConfig dataflow job configuration
   * @return JobMonitor
   */
  @Bean
  public JobMonitor getJobMonitor(
      @Value("${feast.jobs.runner}") String runnerType, DataflowJobConfig dfConfig) {

    Runner runner = Runner.fromString(runnerType);

    switch (runner) {
      case DATAFLOW:
        if (Strings.isNullOrEmpty(dfConfig.getLocation())
            || Strings.isNullOrEmpty(dfConfig.getProjectId())) {
          log.warn(
              "Project and location of the Dataflow runner is not configured, will not do job monitoring");
          return new NoopJobMonitor();
        }
        try {
          GoogleCredential credential =
              GoogleCredential.getApplicationDefault().createScoped(DataflowScopes.all());
          Dataflow dataflow =
              new Dataflow(
                  GoogleNetHttpTransport.newTrustedTransport(),
                  JacksonFactory.getDefaultInstance(),
                  credential);

          return new DataflowJobMonitor(dataflow, dfConfig.getProjectId(), dfConfig.getLocation());
        } catch (IOException e) {
          log.error(
              "Unable to find credential required for Dataflow monitoring API: {}", e.getMessage());
        } catch (GeneralSecurityException e) {
          log.error("Security exception while ");
        } catch (Exception e) {
          log.error("Unable to initialize DataflowJobMonitor", e);
        }
      case FLINK:
      case DIRECT:
      default:
        return new NoopJobMonitor();
    }
  }

  /**
   * Get metrics pusher to statsd
   *
   * @param statsDClient
   * @return StatsdMetricPusher
   */
  @Bean
  public StatsdMetricPusher getStatsdMetricPusher(StatsDClient statsDClient) {
    return new StatsdMetricPusher(statsDClient);
  }
}
