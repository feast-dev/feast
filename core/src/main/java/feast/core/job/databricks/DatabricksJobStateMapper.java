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
import java.util.HashMap;
import java.util.Map;

public class DatabricksJobStateMapper {
  private static final Map<DatabricksJobState, JobStatus> DATABRICKS_TO_FEAST_JOB_STATUS;

  static {
    DATABRICKS_TO_FEAST_JOB_STATUS = new HashMap<>();
    DATABRICKS_TO_FEAST_JOB_STATUS.put(PENDING, JobStatus.PENDING);
    DATABRICKS_TO_FEAST_JOB_STATUS.put(RUNNING, JobStatus.RUNNING);
    //        DATABRICKS_TO_FEAST_JOB_STATUS.put(JOB_STATE_UNKNOWN, JobStatus.UNKNOWN);
    DATABRICKS_TO_FEAST_JOB_STATUS.put(TERMINATING, JobStatus.ABORTING);
    DATABRICKS_TO_FEAST_JOB_STATUS.put(TERMINATED_SUCCESS, JobStatus.COMPLETED);
    DATABRICKS_TO_FEAST_JOB_STATUS.put(TERMINATED_FAILED, JobStatus.ERROR);
    DATABRICKS_TO_FEAST_JOB_STATUS.put(TERMINATED_TIMEDOUT, JobStatus.ABORTED);
    DATABRICKS_TO_FEAST_JOB_STATUS.put(TERMINATED_CANCELED, JobStatus.ABORTED);
    DATABRICKS_TO_FEAST_JOB_STATUS.put(SKIPPED, JobStatus.ABORTED);
    DATABRICKS_TO_FEAST_JOB_STATUS.put(INTERNAL_ERROR, JobStatus.ERROR);
    DATABRICKS_TO_FEAST_JOB_STATUS.put(INTERNAL_ERROR_FAILED, JobStatus.ERROR);
  }

  /**
   * Map a string containing Databricks' JobState into Feast's JobStatus
   *
   * @param jobState Databricks JobState
   * @return JobStatus.
   * @throws IllegalArgumentException if jobState is invalid.
   */
  public static JobStatus map(String jobState) {
    DatabricksJobState dfJobState = DatabricksJobState.valueOf(jobState);
    if (DATABRICKS_TO_FEAST_JOB_STATUS.containsKey(dfJobState)) {
      return DATABRICKS_TO_FEAST_JOB_STATUS.get(dfJobState);
    }
    throw new IllegalArgumentException("Unknown job state: " + jobState);
  }
}
