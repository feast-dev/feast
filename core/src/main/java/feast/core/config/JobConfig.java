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

import feast.core.config.FeastProperties.JobProperties;
import feast.core.job.JobManager;
import feast.core.job.dataflow.DataflowJobManager;
import feast.core.job.direct.DirectJobRegistry;
import feast.core.job.direct.DirectRunnerJobManager;
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
    FeastProperties.JobProperties.Runner runner = jobProperties.getActiveRunner();
    Map<String, String> runnerConfigOptions = runner.getOptions();
    FeastProperties.MetricsProperties metrics = jobProperties.getMetrics();

    switch (runner.getType()) {
      case DATAFLOW:
        return new DataflowJobManager(runnerConfigOptions, metrics);
      case DIRECT:
        return new DirectRunnerJobManager(runnerConfigOptions, directJobRegistry, metrics);
      default:
        throw new IllegalArgumentException("Unsupported runner: " + runner);
    }
  }

  /** Get a direct job registry */
  @Bean
  public DirectJobRegistry directJobRegistry() {
    return new DirectJobRegistry();
  }
}
