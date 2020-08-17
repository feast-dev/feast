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

import feast.jc.model.Job;
import feast.jc.model.JobStatus;
import feast.jc.runner.JobManager;

/**
 * Task that retrieves status from {@link JobManager} on given {@link Job} and update the job
 * accordingly in-place
 */
public class UpdateJobStatusTask extends JobTask {
  public UpdateJobStatusTask(Job job, JobManager jobManager) {
    super(job, jobManager);
  }

  @Override
  public Job call() {
    try {
      JobStatus newStatus = jobManager.getJobStatus(job);
      changeJobStatus(newStatus);

      return job;
    } catch (Exception e) {
      handleException(e);
      return job;
    }
  }
}
