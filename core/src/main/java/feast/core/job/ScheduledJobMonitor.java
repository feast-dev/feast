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

import com.google.common.base.Strings;
import feast.core.dao.JobInfoRepository;
import feast.core.log.Action;
import feast.core.log.AuditLogger;
import feast.core.log.Resource;
import feast.core.model.JobInfo;
import feast.core.model.JobStatus;
import feast.core.model.Metrics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;

@Slf4j
@Component
public class ScheduledJobMonitor {

  private final JobMonitor jobMonitor;
  private final JobInfoRepository jobInfoRepository;
  private final StatsdMetricPusher statsdMetricPusher;

  @Autowired
  public ScheduledJobMonitor(
      JobMonitor jobMonitor,
      JobInfoRepository jobInfoRepository,
      StatsdMetricPusher statsdMetricPusher) {
    this.jobMonitor = jobMonitor;
    this.jobInfoRepository = jobInfoRepository;
    this.statsdMetricPusher = statsdMetricPusher;
  }

  @Scheduled(
      fixedDelayString = "${feast.jobs.monitor.period}",
      initialDelayString = "${feast.jobs.monitor.initialDelay}")
  public void pollStatusAndMetrics() {
    getJobMetrics();
    getJobStatus();
  }

  /** Periodically pull status of job which is not in terminal state and update the status in DB. */
  /* package */ void getJobStatus() {
    if (jobMonitor instanceof NoopJobMonitor) {
      return;
    }

    Collection<JobInfo> nonTerminalJobs =
        jobInfoRepository.findByStatusNotIn(JobStatus.getTerminalState());

    for (JobInfo job : nonTerminalJobs) {
      String jobId = job.getExtId();
      if (Strings.isNullOrEmpty(jobId)) {
        continue;
      }
      JobStatus jobStatus = jobMonitor.getJobStatus(job);
      if (job.getStatus() != jobStatus) {
        AuditLogger.log(
            Resource.JOB,
            jobId,
            Action.STATUS_CHANGE,
            "Job status updated from %s to %s",
            job.getStatus(),
            jobStatus);
      }
      job.setStatus(jobStatus);
      jobInfoRepository.save(job);
    }
  }

  /** Periodically pull metrics of job which is not in terminal state and push it to statsd. */
  /* package */ void getJobMetrics() {
    if (jobMonitor instanceof NoopJobMonitor) {
      return;
    }

    Collection<JobInfo> nonTerminalJobs =
        jobInfoRepository.findByStatusNotIn(JobStatus.getTerminalState());

    for (JobInfo job : nonTerminalJobs) {
      if (Strings.isNullOrEmpty(job.getExtId())) {
        continue;
      }
      List<Metrics> metrics = jobMonitor.getJobMetrics(job);
      if (metrics == null) {
        continue;
      }

      job.setMetrics(metrics);
      statsdMetricPusher.pushMetrics(metrics);
      jobInfoRepository.save(job);
    }
  }
}
