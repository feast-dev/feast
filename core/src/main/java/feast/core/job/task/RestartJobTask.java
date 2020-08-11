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

/** Task that restarts given {@link Job} by restarting it in {@link JobManager} */
@Slf4j
public class RestartJobTask extends JobTask {
  public RestartJobTask(Job job, JobManager jobManager) {
    super(job, jobManager);
  }

  @Override
  public Job call() {
    try {
      // abort job and expect replacement will be spawned
      job = jobManager.abortJob(job);
      log.info("Restart job {} for runner {}", job.getId(), jobManager.getRunnerType().toString());
      AuditLogger.logAction(Level.INFO, JobTasks.ABORT.name(), ResourceType.JOB, job.getId());

      changeJobStatus(JobStatus.ABORTING);
      return job;
    } catch (Exception e) {
      handleException(e);
      return job;
    }
  }
}
