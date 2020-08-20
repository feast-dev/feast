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
package feast.jobcontroller.runner.task;

import feast.common.logging.AuditLogger;
import feast.common.logging.entry.LogResource.ResourceType;
import feast.jobcontroller.model.Job;
import feast.jobcontroller.model.JobStatus;
import feast.jobcontroller.runner.JobManager;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.event.Level;

/** Task to terminate given {@link Job} by using {@link JobManager} */
@Slf4j
public class TerminateJobTask extends JobTask {
  public TerminateJobTask(Job job, JobManager jobManager) {
    super(job, jobManager);
  }

  @Override
  public Job call() {
    try {
      job = jobManager.abortJob(job);
      log.info(
          String.format(
              "Aborted job %s for runner %s", job.getId(), jobManager.getRunnerType().toString()));
      AuditLogger.logAction(Level.INFO, JobTasks.ABORT.name(), ResourceType.JOB, job.getId());

      changeJobStatus(JobStatus.ABORTING);
      return job;
    } catch (Exception e) {
      handleException(e);
      return job;
    }
  }
}
