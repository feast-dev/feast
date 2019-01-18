/*
 * Copyright 2018 The Feast Authors
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
 *
 */

package feast.core.job.flink;

/**
 * Possible state of flink's job.
 */
public enum FlinkJobState {

  /** Job is newly created */
  CREATED,

  /** Job is running */
  RUNNING,

  /** job is completed successfully */
  FINISHED,

  /** job is reset and restarting */
  RESTARTING,

  /** job is being canceled */
  CANCELLING,

  /** job has ben cancelled */
  CANCELED,

  /** job has failed and waiting for cleanup */
  FAILING,

  /** job has failed with a non-recoverable failure */
  FAILED,

  /** job has been suspended and waiting for cleanup */
  SUSPENDING,

  /** job has been suspended */
  SUSPENDED,

  /** job is reconciling and waits for task execution to recover state */
  RECONCILING
}
