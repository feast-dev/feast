/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
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
package feast.core.job;

import feast.core.log.Action;
import feast.core.model.Job;
import feast.core.model.JobStatus;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Task that starts recently created {@link Job} by using {@link JobManager}. */
@Getter
@Setter
@Builder(setterPrefix = "set")
public class CreateJobTask implements JobTask {
  final Logger log = LoggerFactory.getLogger(CreateJobTask.class);

  private Job job;
  private JobManager jobManager;

  @Override
  public Job call() {
    String runnerName = jobManager.getRunnerType().toString();

    job.setRunner(jobManager.getRunnerType());
    job.setStatus(JobStatus.PENDING);

    try {
      JobTask.logAudit(Action.SUBMIT, job, "Building graph and submitting to %s", runnerName);

      job = jobManager.startJob(job);
      var extId = job.getExtId();
      if (extId.isEmpty()) {
        throw new RuntimeException(
            String.format("Could not submit job: \n%s", "unable to retrieve job external id"));
      }

      var auditMessage = "Job submitted to runner %s with ext id %s.";
      JobTask.logAudit(Action.STATUS_CHANGE, job, auditMessage, runnerName, extId);

      return job;
    } catch (Exception e) {
      log.error(e.getMessage());
      var auditMessage = "Job failed to be submitted to runner %s. Job status changed to ERROR.";
      JobTask.logAudit(Action.STATUS_CHANGE, job, auditMessage, runnerName);

      job.setStatus(JobStatus.ERROR);
      return job;
    }
  }
}
