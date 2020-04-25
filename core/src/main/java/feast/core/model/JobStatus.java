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

import feast.core.IngestionJobProto.IngestionJobStatus;
import java.util.Map;
import java.util.Set;

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

  private static final Set<JobStatus> TERMINAL_STATES = Set.of(COMPLETED, ABORTED, ERROR);

  /**
   * Get the set of terminal job states.
   *
   * <p>A terminal job state is final and will not change to any other state.
   *
   * @return set of terminal job states.
   */
  public static Set<JobStatus> getTerminalStates() {
    return TERMINAL_STATES;
  }

  private static final Set<JobStatus> TRANSITIONAL_STATES = Set.of(PENDING, ABORTING, SUSPENDING);

  /**
   * Get Transitional Job Status states. Transitional states are assigned to jobs that are
   * transitioning to a more stable state (ie SUSPENDED, ABORTED etc.)
   *
   * @return set of transitional Job Status states.
   */
  public static Set<JobStatus> getTransitionalStates() {
    return TRANSITIONAL_STATES;
  }

  /** @return true if this {@code JobStatus} is a terminal state. */
  public boolean isTerminal() {
    return getTerminalStates().contains(this);
  }

  /** @return true if this {@code JobStatus} is a transitional state. */
  public boolean isTransitional() {
    return getTransitionalStates().contains(this);
  }

  private static final Map<JobStatus, IngestionJobStatus> INGESTION_JOB_STATUS_MAP =
      Map.of(
          JobStatus.UNKNOWN, IngestionJobStatus.UNKNOWN,
          JobStatus.PENDING, IngestionJobStatus.PENDING,
          JobStatus.RUNNING, IngestionJobStatus.RUNNING,
          JobStatus.COMPLETED, IngestionJobStatus.COMPLETED,
          JobStatus.ABORTING, IngestionJobStatus.ABORTING,
          JobStatus.ABORTED, IngestionJobStatus.ABORTED,
          JobStatus.ERROR, IngestionJobStatus.ERROR,
          JobStatus.SUSPENDING, IngestionJobStatus.SUSPENDING,
          JobStatus.SUSPENDED, IngestionJobStatus.SUSPENDED);

  /**
   * Convert a Job Status to Ingestion Job Status proto
   *
   * @return IngestionJobStatus proto derived from this job status
   */
  public IngestionJobStatus toProto() {
    // maps job models job status to ingestion job status
    return INGESTION_JOB_STATUS_MAP.get(this);
  }
}
