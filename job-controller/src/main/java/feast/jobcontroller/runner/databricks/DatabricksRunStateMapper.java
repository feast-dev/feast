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
package feast.jobcontroller.runner.databricks;

import feast.databricks.types.RunLifeCycleState;
import feast.databricks.types.RunResultState;
import feast.databricks.types.RunState;
import feast.jobcontroller.model.JobStatus;
import java.util.Optional;

public class DatabricksRunStateMapper {

  private DatabricksRunStateMapper() {}

  /**
   * Map a string containing Databricks' RunState into Feast's JobStatus
   *
   * @param runState Databricks RunState
   * @return JobStatus.
   */
  public static JobStatus mapJobState(RunState runState) {
    RunLifeCycleState lifeCycleState = runState.getLifeCycleState();
    Optional<RunResultState> resultState = runState.getResultState();
    switch (lifeCycleState) {
      case INTERNAL_ERROR:
        return JobStatus.ERROR;
      case PENDING:
        return JobStatus.PENDING;
      case RUNNING:
        return JobStatus.RUNNING;
      case SKIPPED:
        return JobStatus.ABORTED;
      case TERMINATED:
        return mapTerminated(resultState, JobStatus.ABORTED);
      case TERMINATING:
        return mapTerminated(resultState, JobStatus.ABORTING);
      default:
        return JobStatus.UNKNOWN;
    }
  }

  private static JobStatus mapTerminated(
      Optional<RunResultState> resultState, JobStatus defaultStatus) {
    if (resultState.isEmpty()) {
      return defaultStatus;
    }
    switch (resultState.get()) {
      case CANCELED:
        return JobStatus.ABORTED;
      case FAILED:
        return JobStatus.ERROR;
      case SUCCESS:
        return JobStatus.COMPLETED;
      case TIMEDOUT:
        return JobStatus.ABORTED;
      default:
        return JobStatus.UNKNOWN;
    }
  }
}
