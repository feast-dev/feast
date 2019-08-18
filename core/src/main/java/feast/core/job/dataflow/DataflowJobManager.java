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
import feast.core.exception.JobExecutionException;
import feast.core.job.JobManager;
import feast.core.model.JobInfo;
import feast.core.util.TypeConversion;
import feast.ingestion.ImportJob;
import feast.ingestion.options.ImportJobPipelineOptions;
import java.io.IOException;
import java.nio.file.Path;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

@Slf4j
public class DataflowJobManager implements JobManager {

  private final String projectId;
  private final String location;
  private final Dataflow dataflow;
  private final ImportJobDefaults defaults;

  public DataflowJobManager(
      Dataflow dataflow, String projectId, String location, ImportJobDefaults importJobDefaults) {
    this.defaults = importJobDefaults;
    checkNotNull(projectId);
    checkNotNull(location);
    this.projectId = projectId;
    this.location = location;
    this.dataflow = dataflow;
  }

  /**
   * Start a new Dataflow Job.
   *
   * @param name job name
   * @param workspace containing specifications for running the job
   * @return Dataflow-specific job id
   */
  @Override
  public String startJob(String name, Path workspace) {
    return submitDataflowJob(workspace, false);
  }

  /**
   * Update an existing Dataflow job.
   *
   * @param jobInfo jobInfo of target job to change
   * @param workspace containing specifications for running the job
   * @return Dataflow-specific job id
   */
  @Override
  public String updateJob(JobInfo jobInfo, Path workspace) {
    return submitDataflowJob(workspace, true);
  }

  /**
   * Abort an existing Dataflow job. Streaming Dataflow jobs are always drained, not cancelled.
   *
   * @param dataflowJobId Dataflow-specific job id (not the job name)
   */
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

  private String submitDataflowJob(Path workspace, boolean update) {
    String[] args = TypeConversion.convertJsonStringToArgs(defaults.getImportJobOptions());

    ImportJobPipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args)
        .as(ImportJobPipelineOptions.class);
    pipelineOptions.setWorkspace(workspace.toUri().toString());
    pipelineOptions
        .setImportJobSpecUri(workspace.resolve("importJobSpecs.yaml").toUri().toString());
    pipelineOptions.setRunner(DataflowRunner.class);
    pipelineOptions.setProject(projectId);
    pipelineOptions.setUpdate(update);

    try {
      DataflowPipelineJob pipelineResult = runPipeline(pipelineOptions);
      String jobId = waitForJobToRun(pipelineResult);
      return jobId;
    } catch (Exception e) {
      log.error("Error submitting job", e);
      throw new JobExecutionException(String.format("Error running ingestion job: %s", e), e);
    }
  }

  public DataflowPipelineJob runPipeline(ImportJobPipelineOptions pipelineOptions)
      throws IOException {
    return (DataflowPipelineJob) ImportJob
        .runPipeline(pipelineOptions);
  }

  private String waitForJobToRun(DataflowPipelineJob pipelineResult)
      throws RuntimeException, InterruptedException {
    // TODO: add timeout
    while (true) {
      State state = pipelineResult.getState();
      if (state.isTerminal()) {
        String dataflowDashboardUrl = String
            .format("https://console.cloud.google.com/dataflow/jobsDetail/locations/%s/jobs/%s",
                location, pipelineResult.getJobId());
        throw new RuntimeException(
            String.format(
                "Failed to submit dataflow job, job state is %s. Refer to the dataflow dashboard for more information: %s",
                state.toString(), dataflowDashboardUrl));
      } else if (state.equals(State.RUNNING)) {
        return pipelineResult.getJobId();
      }
      Thread.sleep(2000);
    }
  }
}
