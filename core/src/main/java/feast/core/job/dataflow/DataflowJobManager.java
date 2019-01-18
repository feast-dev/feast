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

package feast.core.job.dataflow;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import com.google.common.base.Strings;
import feast.core.config.ImportJobDefaults;
import feast.core.job.direct.DirectRunnerJobManager;
import feast.specs.ImportSpecProto.ImportSpec;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataflowJobManager extends DirectRunnerJobManager {

  private final String projectId;
  private final String location;
  private final Dataflow dataflow;
  private ImportJobDefaults defaults;

  public DataflowJobManager(
      Dataflow dataflow, String projectId, String location, ImportJobDefaults importJobDefaults) {
    super(importJobDefaults);
    checkNotNull(projectId);
    checkNotNull(location);
    this.projectId = projectId;
    this.location = location;
    this.dataflow = dataflow;
    this.defaults = importJobDefaults;
  }

  @Override
  public String submitJob(ImportSpec importSpec, String jobId) {
    return super.submitJob(importSpec, jobId);
  }

  @Override
  public void abortJob(String dataflowJobId) {
    try {
      Job job =
          dataflow.projects().locations().jobs().get(projectId, location, dataflowJobId).execute();
      Job content = new Job();
      if (job.getType().equals(DataflowJobType.JOB_TYPE_BATCH.toString())) {
        content.setRequestedState(DataflowJobState.JOB_STATE_CANCELLED.toString());
      } else if (job.getType().equals(DataflowJobType.JOB_TYPE_STREAMING.toString())) {
        content.setRequestedState(DataflowJobState.JOB_STATE_DRAINING.toString());
      }
      dataflow
          .projects()
          .locations()
          .jobs()
          .update(projectId, location, dataflowJobId, content)
          .execute();
    } catch (Exception e) {
      log.error("Unable to drain job with id: {}, cause: {}", dataflowJobId, e.getMessage());
      throw new RuntimeException(
          Strings.lenientFormat("Unable to drain job with id: %s", dataflowJobId), e);
    }
  }
}
