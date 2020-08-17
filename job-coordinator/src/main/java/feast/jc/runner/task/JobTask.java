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
package feast.jc.runner.task;

import feast.common.logging.AuditLogger;
import feast.common.logging.entry.LogResource.ResourceType;
import feast.jc.model.Job;
import feast.jc.model.JobStatus;
import feast.jc.runner.JobManager;
import java.util.concurrent.Callable;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.event.Level;

@Getter
@Setter
@Slf4j
public abstract class JobTask implements Callable<Job> {
  protected Job job;
  protected JobManager jobManager;

  public JobTask(Job job, JobManager jobManager) {
    this.job = job;
    this.jobManager = jobManager;
  }

  @Override
  public abstract Job call() throws RuntimeException;

  /**
   * Change Job Status to the given status and logs changes in Job Status to audit and normal log.
   */
  protected void changeJobStatus(JobStatus newStatus) {
    JobStatus currentStatus = job.getStatus();
    if (currentStatus != newStatus) {
      job.setStatus(newStatus);
      log.info(
          String.format("Job status updated: changed from %s to %s", currentStatus, newStatus));

      AuditLogger.logTransition(Level.INFO, newStatus.name(), ResourceType.JOB, job.getId());
      log.info("test");
    }
  }

  /**
   * Handle Exception when executing JobTask by transition Job to ERROR status and logging exception
   */
  protected void handleException(Exception e) {
    log.error("Unexpected exception performing JobTask: %s", e.getMessage());
    e.printStackTrace();
    changeJobStatus(JobStatus.ERROR);
  }
}
