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
package feast.core.job.task;

import feast.common.logging.AuditLogger;
import feast.common.logging.entry.LogResource.ResourceType;
import feast.core.job.JobManager;
import feast.core.model.Job;
import feast.core.model.JobStatus;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.event.Level;

/** Task that starts recently created {@link Job} by using {@link feast.core.job.JobManager}. */
@Slf4j
public class CreateJobTask extends JobTask {

  public CreateJobTask(Job job, JobManager jobManager) {
    super(job, jobManager);
  }

  @Override
  public Job call() {
    try {
      String runnerName = jobManager.getRunnerType().toString();
      changeJobStatus(JobStatus.PENDING);

      // Start job with jobManager.
      job = jobManager.startJob(job);

      log.info(String.format("Build graph and submitting to %s", runnerName));
      AuditLogger.logAction(Level.INFO, JobTasks.CREATE.name(), ResourceType.JOB, job.getId());

      // Check for expected external job id
      if (job.getExtId().isEmpty()) {
        throw new RuntimeException(
            String.format(
                "Could not submit job %s: unable to retrieve job external id", job.getId()));
      }

      log.info(
          String.format("Job submitted to runner %s with ext id %s.", runnerName, job.getExtId()));
      changeJobStatus(JobStatus.RUNNING);
      return job;
    } catch (Exception e) {
      handleException(e);
      return job;
    }
  }
}
