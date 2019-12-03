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

public enum DataflowJobState {
  JOB_STATE_UNKNOWN,
  JOB_STATE_STOPPED,
  JOB_STATE_RUNNING,
  JOB_STATE_DONE,
  JOB_STATE_FAILED,
  JOB_STATE_CANCELLED,
  JOB_STATE_UPDATED,
  JOB_STATE_DRAINING,
  JOB_STATE_DRAINED,
  JOB_STATE_PENDING,
  JOB_STATE_CANCELLING
}
