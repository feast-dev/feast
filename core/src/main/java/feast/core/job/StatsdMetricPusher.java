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

package feast.core.job;

import com.timgroup.statsd.StatsDClient;
import feast.core.model.Metrics;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Metrics pusher to statsd
 */
public class StatsdMetricPusher {
  private final StatsDClient statsDClient;

  @Autowired
  public StatsdMetricPusher(StatsDClient statsDClient) {
    this.statsDClient = statsDClient;
  }

  /**
   * Push metrics to statsd
   * @param metrics list of Metrics from the runner
   */
  public void pushMetrics(List<Metrics> metrics) {
    for (Metrics metric : metrics) {
      // feast metrics is delimited by colon (:)
      // `row:result` <- all
      // `scope:scope_id:result` <- per scope, either feature or entity
      // for examples:
      // 1. feature:driver.ping_sequence_weak_dir_change_mean:error
      // 2. entity:driver:stored
      // currently there are 3 results:
      // 1. valid
      // 2. error
      // 3. stored
      String[] splittedName = metric.getName().split(":");
      String jobId = metric.getJobInfo().getId();
      String scope = splittedName[0];
      switch (scope) {
        case "row":
          String type = splittedName[1];
          statsDClient
              .gauge(createScope(scope), metric.getValue(),
                  createTag("jobId", jobId),
                  createTag("type", type));
          break;
        case "feature":
          String featureId = splittedName[1];
          String result = splittedName[2];
          statsDClient
              .gauge(createScope(scope), metric.getValue(),
                  createTag("jobId", jobId),
                  createTag("featureId", featureId),
                  createTag("result", result));
          break;
        case "entity":
          String entityName = splittedName[1];
          String opResult = splittedName[2];
          statsDClient
              .gauge(createScope(scope), metric.getValue(),
                  createTag("jobId", jobId),
                  createTag("entityName", entityName),
                  createTag("result", opResult));
          break;
      }
    }
  }

  private String createTag(String tagName, String tagValue) {
    return tagName + ":" + tagValue;
  }

  private String createScope(String scopeName) {
    return scopeName + "_metric";
  }
}
