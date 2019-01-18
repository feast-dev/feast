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

package feast.core.job.flink;

import feast.core.job.JobMonitor;
import feast.core.job.Runner;
import feast.core.model.JobInfo;
import feast.core.model.JobStatus;
import feast.core.model.Metrics;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FlinkJobMonitor implements JobMonitor {

  private final FlinkRestApi flinkRestApi;
  private final FlinkJobMapper mapper;

  public FlinkJobMonitor(FlinkRestApi flinkRestApi) {
    this.flinkRestApi = flinkRestApi;
    this.mapper = new FlinkJobMapper();
  }

  @Override
  public JobStatus getJobStatus(JobInfo jobInfo) {
    if (!Runner.FLINK.getName().equals(jobInfo.getRunner())) {
      return jobInfo.getStatus();
    }

    FlinkJobList jobList = flinkRestApi.getJobsOverview();
    for (FlinkJob job : jobList.getJobs()) {
      if (jobInfo.getExtId().equals(job.getJid())) {
        return mapFlinkJobStatusToFeastJobStatus(job.getState());
      }
    }
    return JobStatus.UNKNOWN;
  }

  @Override
  public List<Metrics> getJobMetrics(JobInfo job) {
    if (!Runner.FLINK.getName().equals(job.getRunner())) {
      return null;
    }
    // TODO: metrics for flink
    return Collections.emptyList();
  }

  private JobStatus mapFlinkJobStatusToFeastJobStatus(String state) {
    try {
      return mapper.map(state);
    } catch (IllegalArgumentException e) {
      log.error("Unknown job state: " + state);
      return JobStatus.UNKNOWN;
    }
  }
}
