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
package feast.core.job;

import feast.core.model.Job;
import feast.core.model.JobStatus;

public interface JobManager {

  /**
   * Get Runner Type
   *
   * @return runner type
   */
  Runner getRunnerType();

  /**
   * Start an import job. Start should change the status of the Job from PENDING to RUNNING.
   *
   * @param job job to start
   * @return Job
   */
  Job startJob(Job job);

  /**
   * Update already running job with new set of features to ingest.
   *
   * @param job job of target job to change
   * @return Job
   */
  Job updateJob(Job job);

  /**
   * Abort a job given runner-specific job ID. Abort should change the status of the Job from
   * RUNNING to ABORTING.
   *
   * @param job to abort.
   * @return The aborted Job
   */
  Job abortJob(Job job);

  /**
   * Restart an job. If job is an terminated state, will simply start the job. Might cause data to
   * be lost during when restarting running jobs in some implementations. Refer to on docs the
   * specific implementation.
   *
   * @param job job to restart
   * @return the restarted job
   */
  Job restartJob(Job job);

  /**
   * Get status of a job given runner-specific job ID.
   *
   * @param job job.
   * @return job status.
   */
  JobStatus getJobStatus(Job job);
}
