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

import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import com.google.common.base.Strings;
import feast.core.job.JobManager;
import lombok.extern.slf4j.Slf4j;

import static com.google.common.base.Preconditions.checkNotNull;

@Slf4j
public class DataflowJobManager implements JobManager {

  private final String projectId;
  private final String location;
  private final Dataflow dataflow;

  public DataflowJobManager(Dataflow dataflow, String projectId, String location) {
    checkNotNull(projectId);
    checkNotNull(location);
    this.projectId = projectId;
    this.location = location;
    this.dataflow = dataflow;
  }

  @Override
  public void abortJob(String dataflowJobId) {
    try {
      Job job =
          dataflow.projects().locations().jobs().get(projectId, location, dataflowJobId).execute();
      Job content = new Job();
      if (job.getType().equals("JOB_TYPE_BATCH")){
        content.setRequestedState(DataflowJobState.JOB_STATE_CANCELLED.toString());
      } else if (job.getType().equals("JOB_TYPE_STREAMING")) {
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
