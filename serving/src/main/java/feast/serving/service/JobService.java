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
package feast.serving.service;

import feast.serving.ServingAPIProto.Job;
import java.util.Optional;

// JobService interface specifies the operations to manage Job instances internally in Feast

public interface JobService {

  /**
   * Get Job by job id.
   *
   * @param id job id
   * @return feast.serving.ServingAPIProto.Job
   */
  Optional<Job> get(String id);

  /**
   * Update or create a job (if not exists)
   *
   * @param job feast.serving.ServingAPIProto.Job
   */
  void upsert(Job job);
}
