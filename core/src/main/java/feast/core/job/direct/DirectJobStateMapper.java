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
package feast.core.job.direct;

import feast.core.model.JobStatus;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.PipelineResult.State;

public class DirectJobStateMapper {

  private static final Map<State, JobStatus> BEAM_TO_FEAT_JOB_STATUS;

  static {
    BEAM_TO_FEAT_JOB_STATUS = new HashMap<>();
    BEAM_TO_FEAT_JOB_STATUS.put(State.FAILED, JobStatus.ERROR);
    BEAM_TO_FEAT_JOB_STATUS.put(State.RUNNING, JobStatus.RUNNING);
    BEAM_TO_FEAT_JOB_STATUS.put(State.UNKNOWN, JobStatus.UNKNOWN);
    BEAM_TO_FEAT_JOB_STATUS.put(State.CANCELLED, JobStatus.ABORTED);
    BEAM_TO_FEAT_JOB_STATUS.put(State.DONE, JobStatus.COMPLETED);
    BEAM_TO_FEAT_JOB_STATUS.put(State.STOPPED, JobStatus.ABORTED);
    BEAM_TO_FEAT_JOB_STATUS.put(State.UPDATED, JobStatus.RUNNING);
  }

  /**
   * Map a dataflow job state to Feast's JobStatus
   *
   * @param jobState beam PipelineResult State
   * @return JobStatus
   */
  public static JobStatus map(State jobState) {
    return BEAM_TO_FEAT_JOB_STATUS.get(jobState);
  }
}
