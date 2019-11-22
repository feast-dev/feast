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

package feast.core.job;

import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.StoreProto.Store;
import feast.core.model.JobInfo;
import java.util.List;

public interface JobManager {

  /**
   * Get Runner Type
   *
   * @return runner type
   */
  Runner getRunnerType();

  /**
   * Start an import job.
   *
   * @param name of job to run
   * @param featureSets list of featureSets to be populated by the job
   * @param sink Store to sink features to
   * @return runner specific job id
   */
  String startJob(String name, List<FeatureSetSpec> featureSets, Store sink);

  /**
   * Update already running job with new set of features to ingest.
   *
   * @param jobInfo jobInfo of target job to change
   * @return job runner specific job id
   */
  String updateJob(JobInfo jobInfo);

  /**
   * Abort a job given runner-specific job ID.
   *
   * @param extId runner specific job id.
   */
  void abortJob(String extId);
}
