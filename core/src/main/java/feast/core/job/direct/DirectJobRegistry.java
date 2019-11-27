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

import com.google.common.base.Strings;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Singleton
public class DirectJobRegistry {

  private Map<String, DirectJob> jobs;

  public DirectJobRegistry() {
    this.jobs = new HashMap<>();
  }

  /**
   * Add the given job to the registry.
   *
   * @param job containing the job id,
   */
  public void add(DirectJob job) {
    if (jobs.containsKey(job.getJobId())) {
      throw new IllegalArgumentException(
          Strings.lenientFormat("Job with id %s already exists and is running", job.getJobId()));
    }
    jobs.put(job.getJobId(), job);
  }

  /**
   * Get DirectJob corresponding to the given ID
   *
   * @param id of the job to retrieve
   * @return DirectJob
   */
  public DirectJob get(String id) {
    return jobs.getOrDefault(id, null);
  }

  /**
   * Remove DirectJob corresponding to the given ID
   *
   * @param id of the job to remove
   */
  public void remove(String id) {
    jobs.remove(id);
  }

  /** Kill all child jobs when the registry is garbage collected */
  @Override
  public void finalize() {
    for (DirectJob job : this.jobs.values()) {
      try {
        job.getPipelineResult().cancel();
      } catch (IOException e) {
        log.error("Failed to stop job", e);
      }
    }
  }
}
