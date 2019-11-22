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
import java.util.Collection;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
public class ScheduledJobMonitor {

  private final JobMonitor jobMonitor;
  private final JobInfoRepository jobInfoRepository;

  @Autowired
  public ScheduledJobMonitor(JobMonitor jobMonitor, JobInfoRepository jobInfoRepository) {
    this.jobMonitor = jobMonitor;
    this.jobInfoRepository = jobInfoRepository;
  }

  // TODO: Keep receiving the following exception with these arguments below
  //       Caused by: java.lang.IllegalStateException: Encountered invalid @Scheduled method
  // 'pollStatusAndMetrics': Circular placeholder reference .. in property definitions
  // @Scheduled(
  //    fixedDelayString = "${feast.jobs.monitor.fixedDelay}",
  //    initialDelayString = "${feast.jobs.monitor.initialDelay}")
  //
  @Transactional
  @Scheduled(cron = "* * * * * *")
  public void pollStatusAndMetrics() {
    updateJobStatus();
  }

  /** Periodically pull status of job which is not in terminal state and update the status in DB. */
  /* package */ void updateJobStatus() {
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
}
