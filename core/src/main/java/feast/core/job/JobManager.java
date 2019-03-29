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

import feast.specs.ImportJobSpecsProto.ImportJobSpecs;
import java.nio.file.Path;

public interface JobManager {

  /**
   * Submit an ingestion job into runner
   *
   * @param importJobSpecs wrapper of all the specs needed for the ingestion job to run
   * @param workspace path for working directory of the job, for errors and specs
   * @return extId runner specific job ID.
   */
  String submitJob(ImportJobSpecs importJobSpecs, Path workspace);

  /**
   * abort a job given runner-specific job ID.
   *
   * @param extId runner specific job id.
   */
  void abortJob(String extId);
}
