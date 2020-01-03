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
package feast.core.job.dataflow;

import static feast.core.job.dataflow.DataflowJobState.JOB_STATE_CANCELLED;
import static feast.core.job.dataflow.DataflowJobState.JOB_STATE_CANCELLING;
import static feast.core.job.dataflow.DataflowJobState.JOB_STATE_DONE;
import static feast.core.job.dataflow.DataflowJobState.JOB_STATE_DRAINED;
import static feast.core.job.dataflow.DataflowJobState.JOB_STATE_DRAINING;
import static feast.core.job.dataflow.DataflowJobState.JOB_STATE_FAILED;
import static feast.core.job.dataflow.DataflowJobState.JOB_STATE_PENDING;
import static feast.core.job.dataflow.DataflowJobState.JOB_STATE_RUNNING;
import static feast.core.job.dataflow.DataflowJobState.JOB_STATE_STOPPED;
import static feast.core.job.dataflow.DataflowJobState.JOB_STATE_UNKNOWN;
import static feast.core.job.dataflow.DataflowJobState.JOB_STATE_UPDATED;

import feast.core.model.JobStatus;
import java.util.HashMap;
import java.util.Map;

public class DataflowJobStateMapper {
  private static final Map<DataflowJobState, JobStatus> DATAFLOW_TO_FEAST_JOB_STATUS;

  static {
    DATAFLOW_TO_FEAST_JOB_STATUS = new HashMap<>();
    DATAFLOW_TO_FEAST_JOB_STATUS.put(JOB_STATE_UNKNOWN, JobStatus.UNKNOWN);
    // Dataflow: JOB_STATE_STOPPED indicates that the job has not yet started to run.
    DATAFLOW_TO_FEAST_JOB_STATUS.put(JOB_STATE_STOPPED, JobStatus.PENDING);
    DATAFLOW_TO_FEAST_JOB_STATUS.put(JOB_STATE_PENDING, JobStatus.PENDING);
    DATAFLOW_TO_FEAST_JOB_STATUS.put(JOB_STATE_RUNNING, JobStatus.RUNNING);
    DATAFLOW_TO_FEAST_JOB_STATUS.put(JOB_STATE_UPDATED, JobStatus.RUNNING);
    DATAFLOW_TO_FEAST_JOB_STATUS.put(JOB_STATE_DRAINING, JobStatus.ABORTING);
    DATAFLOW_TO_FEAST_JOB_STATUS.put(JOB_STATE_CANCELLING, JobStatus.ABORTING);
    DATAFLOW_TO_FEAST_JOB_STATUS.put(JOB_STATE_DRAINED, JobStatus.ABORTED);
    DATAFLOW_TO_FEAST_JOB_STATUS.put(JOB_STATE_CANCELLED, JobStatus.ABORTED);
    DATAFLOW_TO_FEAST_JOB_STATUS.put(JOB_STATE_FAILED, JobStatus.ERROR);
    DATAFLOW_TO_FEAST_JOB_STATUS.put(JOB_STATE_DONE, JobStatus.COMPLETED);
  }

  /**
   * Map a string containing Dataflow's JobState into Feast's JobStatus
   *
   * @param jobState Dataflow JobState
   * @return JobStatus.
   * @throws IllegalArgumentException if jobState is invalid.
   */
  public static JobStatus map(String jobState) {
    DataflowJobState dfJobState = DataflowJobState.valueOf(jobState);
    if (DATAFLOW_TO_FEAST_JOB_STATUS.containsKey(dfJobState)) {
      return DATAFLOW_TO_FEAST_JOB_STATUS.get(dfJobState);
    }
    throw new IllegalArgumentException("Unknown job state: " + jobState);
  }
}
