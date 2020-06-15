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
package feast.core.job.databricks;

import static feast.core.job.databricks.DatabricksJobState.*;

import feast.core.model.JobStatus;
import java.util.Map;
import java.util.Optional;

public class DatabricksJobStateMapper {
  private DatabricksJobStateMapper() {}

  private static final Map<DatabricksJobState, JobStatus> DATABRICKS_TO_FEAST_JOB_STATUS =
      Map.of(
          PENDING, JobStatus.PENDING,
          RUNNING, JobStatus.RUNNING,
          //      JOB_STATE_UNKNOWN, JobStatus.UNKNOWN
          TERMINATING, JobStatus.ABORTING,
          TERMINATED_SUCCESS, JobStatus.COMPLETED,
          TERMINATED_FAILED, JobStatus.ERROR,
          TERMINATED_TIMEDOUT, JobStatus.ABORTED,
          TERMINATED_CANCELED, JobStatus.ABORTED,
          SKIPPED, JobStatus.ABORTED,
          INTERNAL_ERROR, JobStatus.ERROR,
          INTERNAL_ERROR_FAILED, JobStatus.ERROR);
  /**
   * Map a string containing Databricks' JobState into Feast's JobStatus
   *
   * @param jobState Databricks JobState
   * @return JobStatus.
   * @throws IllegalArgumentException if jobState is invalid.
   */
  public static Optional<JobStatus> mapJobState(String jobState) {
    DatabricksJobState dfJobState = DatabricksJobState.valueOf(jobState);
    if (DATABRICKS_TO_FEAST_JOB_STATUS.containsKey(dfJobState)) {
      return Optional.of(DATABRICKS_TO_FEAST_JOB_STATUS.get(dfJobState));
    }
    return Optional.empty();
  }
}
