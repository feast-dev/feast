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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.DataflowScopes;
import com.google.common.base.Strings;
import feast.core.config.FeastProperties.JobProperties;
import feast.core.job.JobManager;
import feast.core.job.Runner;
import feast.core.job.dataflow.DataflowJobManager;
import feast.core.job.direct.DirectJobRegistry;
import feast.core.job.direct.DirectRunnerJobManager;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Beans for job management */
@Slf4j
@Configuration
public class JobConfig {

  /**
   * Get a JobManager according to the runner type and dataflow configuration.
   *
   * @param feastProperties feast config properties
   */
  @Bean
  @Autowired
  public JobManager getJobManager(
      FeastProperties feastProperties, DirectJobRegistry directJobRegistry) {

    JobProperties jobProperties = feastProperties.getJobs();
    Runner runner = Runner.fromString(jobProperties.getRunner());
    Map<String, String> jobOptions = jobProperties.getRunnerOptionsMap();
    switch (runner) {
      case DATAFLOW:
        if (Strings.isNullOrEmpty(jobOptions.getOrDefault("region", null))
            || Strings.isNullOrEmpty(jobOptions.getOrDefault("project", null))) {
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
              dataflow, jobProperties.getRunnerOptionsMap(), jobProperties.getMetrics());
        } catch (IOException e) {
          throw new IllegalStateException(
              "Unable to find credential required for Dataflow monitoring API", e);
        } catch (GeneralSecurityException e) {
          throw new IllegalStateException("Security exception while connecting to Dataflow API", e);
        } catch (Exception e) {
          throw new IllegalStateException("Unable to initialize DataflowJobManager", e);
        }
      case DIRECT:
        return new DirectRunnerJobManager(
            jobProperties.getRunnerOptionsMap(), directJobRegistry, jobProperties.getMetrics());
      default:
        throw new IllegalArgumentException("Unsupported runner: " + jobProperties.getRunner());
    }
  }

  /** Get a direct job registry */
  @Bean
  public DirectJobRegistry directJobRegistry() {
    return new DirectJobRegistry();
  }
}
