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
package feast.core.model;

import com.google.common.collect.ImmutableMap;
import feast.core.IngestionJobProto.IngestionJobStatus;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public enum JobStatus {
  /** Job status is not known. */
  UNKNOWN,

  /** Import job is submitted to runner and currently pending for executing */
  PENDING,

  /** Import job is currently running in the runner */
  RUNNING,

  /** Runner’s reported the import job has completed (applicable to batch job) */
  COMPLETED,

  /** When user sent abort command, but it's still running */
  ABORTING,

  /** User initiated abort job */
  ABORTED,

  /**
   * Runner’s reported that the import job failed to run or there is a failure during job
   * submission.
   */
  ERROR,

  /** job has been suspended and waiting for cleanup */
  SUSPENDING,

  /** job has been suspended */
  SUSPENDED;

  private static final Collection<JobStatus> TERMINAL_STATE =
      Collections.unmodifiableList(Arrays.asList(COMPLETED, ABORTED, ERROR));

  /**
   * Get a collection of terminal job state.
   *
   * <p>Terminal job state is final and will not change to any other state.
   *
   * @return collection of terminal job state.
   */
  public static Collection<JobStatus> getTerminalState() {
    return TERMINAL_STATE;
  }

  private static final Collection<JobStatus> TRANSITIONAL_STATES =
      Collections.unmodifiableList(Arrays.asList(PENDING, ABORTING, SUSPENDING));

  /**
   * Get Transitional Job Status states. Transitionals states are assigned to jobs that
   * transitioning to a more stable state (ie SUSPENDED, ABORTED etc.)
   *
   * @return Collection of transitional Job Status states.
   */
  public static final Collection<JobStatus> getTransitionalStates() {
    return TRANSITIONAL_STATES;
  }

  private static final Map<JobStatus, IngestionJobStatus> INGESTION_JOB_STATUS_MAP =
      ImmutableMap.<JobStatus, IngestionJobStatus>builder()
          .put(JobStatus.UNKNOWN, IngestionJobStatus.UNKNOWN)
          .put(JobStatus.PENDING, IngestionJobStatus.PENDING)
          .put(JobStatus.RUNNING, IngestionJobStatus.RUNNING)
          .put(JobStatus.COMPLETED, IngestionJobStatus.COMPLETED)
          .put(JobStatus.ABORTING, IngestionJobStatus.ABORTING)
          .put(JobStatus.ABORTED, IngestionJobStatus.ABORTED)
          .put(JobStatus.ERROR, IngestionJobStatus.ERROR)
          .put(JobStatus.SUSPENDING, IngestionJobStatus.SUSPENDING)
          .put(JobStatus.SUSPENDED, IngestionJobStatus.SUSPENDED)
          .build();

  /**
   * Convert a Job Status to Ingestion Job Status proto
   *
   * @return IngestionJobStatus proto derieved from this job status
   */
  public IngestionJobStatus toProto() {
    // maps job models job status to ingestion job status
    return INGESTION_JOB_STATUS_MAP.get(this);
  }
}
