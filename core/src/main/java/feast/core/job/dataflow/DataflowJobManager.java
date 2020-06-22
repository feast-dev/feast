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

import static feast.core.util.PipelineUtil.detectClassPathResourcesToStage;
import static feast.core.util.StreamUtil.wrapException;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.DataflowScopes;
import com.google.common.base.Strings;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import feast.core.config.FeastProperties.MetricsProperties;
import feast.core.exception.JobExecutionException;
import feast.core.job.JobManager;
import feast.core.job.Runner;
import feast.core.model.*;
import feast.ingestion.ImportJob;
import feast.ingestion.options.ImportOptions;
import feast.proto.core.IngestionJobProto;
import feast.proto.core.RunnerProto.DataflowRunnerConfigOptions;
import feast.proto.core.SourceProto;
import feast.proto.core.StoreProto;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

@Slf4j
public class DataflowJobManager implements JobManager {

  private final Runner RUNNER_TYPE = Runner.DATAFLOW;

  private final String projectId;
  private final String location;
  private final Dataflow dataflow;
  private final DataflowRunnerConfig defaultOptions;
  private final MetricsProperties metrics;
  private final IngestionJobProto.SpecsStreamingUpdateConfig specsStreamingUpdateConfig;

  public DataflowJobManager(
      DataflowRunnerConfigOptions runnerConfigOptions,
      MetricsProperties metricsProperties,
      IngestionJobProto.SpecsStreamingUpdateConfig specsStreamingUpdateConfig) {
    this(runnerConfigOptions, metricsProperties, specsStreamingUpdateConfig, getGoogleCredential());
  }

  public DataflowJobManager(
      DataflowRunnerConfigOptions runnerConfigOptions,
      MetricsProperties metricsProperties,
      IngestionJobProto.SpecsStreamingUpdateConfig specsStreamingUpdateConfig,
      Credential credential) {

    defaultOptions = new DataflowRunnerConfig(runnerConfigOptions);
    Dataflow dataflow = null;
    try {
      dataflow =
          new Dataflow(
              GoogleNetHttpTransport.newTrustedTransport(),
              JacksonFactory.getDefaultInstance(),
              credential);
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("Security exception while connecting to Dataflow API", e);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to initialize DataflowJobManager", e);
    }

    this.dataflow = dataflow;
    this.metrics = metricsProperties;
    this.projectId = defaultOptions.getProject();
    this.location = defaultOptions.getRegion();
    this.specsStreamingUpdateConfig = specsStreamingUpdateConfig;
  }

  private static Credential getGoogleCredential() {
    GoogleCredential credential = null;
    try {
      credential = GoogleCredential.getApplicationDefault().createScoped(DataflowScopes.all());
    } catch (IOException e) {
      throw new IllegalStateException(
          "Unable to find credential required for Dataflow monitoring API", e);
    }
    return credential;
  }

  @Override
  public Runner getRunnerType() {
    return RUNNER_TYPE;
  }

  @Override
  public Job startJob(Job job) {
    try {
      String extId =
          submitDataflowJob(
              job.getId(),
              job.getSource().toProto(),
              job.getStores().stream()
                  .map(wrapException(Store::toProto))
                  .collect(Collectors.toSet()),
              false);
      job.setExtId(extId);
      job.setStatus(JobStatus.RUNNING);
      return job;

    } catch (RuntimeException e) {
      log.error(e.getMessage());
      if (e.getCause() instanceof InvalidProtocolBufferException) {
        throw new IllegalArgumentException(
            String.format(
                "DataflowJobManager failed to START job with id '%s' because the job"
                    + "has an invalid spec. Please check the FeatureSet, Source and Store specs. Actual error message: %s",
                job.getId(), e.getMessage()));
      }

      throw e;
    }
  }

  /**
   * Drain existing job. Replacement will be created on next run (when job gracefully stop)
   *
   * @param job job of target job to change
   * @return same job as input
   */
  @Override
  public Job updateJob(Job job) {
    abortJob(job);
    return job;
  }

  /**
   * Abort an existing Dataflow job. Streaming Dataflow jobs are always drained, not cancelled.
   *
   * @param job to abort.
   * @return The aborted Job.
   */
  @Override
  public Job abortJob(Job job) {
    String dataflowJobId = job.getExtId();
    try {
      com.google.api.services.dataflow.model.Job dataflowJob =
          dataflow.projects().locations().jobs().get(projectId, location, dataflowJobId).execute();
      com.google.api.services.dataflow.model.Job content =
          new com.google.api.services.dataflow.model.Job();
      if (dataflowJob.getType().equals(DataflowJobType.JOB_TYPE_BATCH.toString())) {
        content.setRequestedState(DataflowJobState.JOB_STATE_CANCELLED.toString());
      } else if (dataflowJob.getType().equals(DataflowJobType.JOB_TYPE_STREAMING.toString())) {
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

    job.setStatus(JobStatus.ABORTING);
    return job;
  }

  /**
   * Restart a Dataflow job. Dataflow should ensure continuity such that no data should be lost
   * during the restart operation.
   *
   * @param job job to restart
   * @return the restarted job
   */
  @Override
  public Job restartJob(Job job) {
    if (job.getStatus().isTerminal()) {
      // job yet not running: just start job
      return this.startJob(job);
    } else {
      // job is running - updating the job without changing the job has
      // the effect of restarting the job
      return this.updateJob(job);
    }
  }

  /**
   * Get status of a dataflow job with given id and try to map it into Feast's JobStatus.
   *
   * @param job Job containing dataflow job id
   * @return status of the job, or return {@link JobStatus#UNKNOWN} if error happens.
   */
  @Override
  public JobStatus getJobStatus(Job job) {
    if (job.getRunner() != RUNNER_TYPE) {
      return job.getStatus();
    }

    try {
      com.google.api.services.dataflow.model.Job dataflowJob =
          dataflow.projects().locations().jobs().get(projectId, location, job.getExtId()).execute();
      return DataflowJobStateMapper.map(dataflowJob.getCurrentState());
    } catch (Exception e) {
      log.error(
          "Unable to retrieve status of a dataflow job with id : {}\ncause: {}",
          job.getExtId(),
          e.getMessage());
    }
    return JobStatus.UNKNOWN;
  }

  private String submitDataflowJob(
      String jobName, SourceProto.Source source, Set<StoreProto.Store> sinks, boolean update) {
    try {
      ImportOptions pipelineOptions = getPipelineOptions(jobName, source, sinks, update);
      DataflowPipelineJob pipelineResult = runPipeline(pipelineOptions);
      String jobId = waitForJobToRun(pipelineResult);
      return jobId;
    } catch (Exception e) {
      log.error("Error submitting job", e);
      throw new JobExecutionException(String.format("Error running ingestion job: %s", e), e);
    }
  }

  private ImportOptions getPipelineOptions(
      String jobName, SourceProto.Source source, Set<StoreProto.Store> sinks, boolean update)
      throws IOException, IllegalAccessException {
    ImportOptions pipelineOptions =
        PipelineOptionsFactory.fromArgs(defaultOptions.toArgs()).as(ImportOptions.class);

    JsonFormat.Printer jsonPrinter = JsonFormat.printer();

    pipelineOptions.setSpecsStreamingUpdateConfigJson(
        jsonPrinter.print(specsStreamingUpdateConfig));
    pipelineOptions.setSourceJson(jsonPrinter.print(source));
    pipelineOptions.setStoresJson(
        sinks.stream().map(wrapException(jsonPrinter::print)).collect(Collectors.toList()));
    pipelineOptions.setProject(projectId);
    pipelineOptions.setDefaultFeastProject(Project.DEFAULT_NAME);
    pipelineOptions.setUpdate(update);
    pipelineOptions.setRunner(DataflowRunner.class);
    pipelineOptions.setJobName(jobName);
    pipelineOptions.setFilesToStage(
        detectClassPathResourcesToStage(DataflowRunner.class.getClassLoader()));
    if (metrics.isEnabled()) {
      pipelineOptions.setMetricsExporterType(metrics.getType());
      if (metrics.getType().equals("statsd")) {
        pipelineOptions.setStatsdHost(metrics.getHost());
        pipelineOptions.setStatsdPort(metrics.getPort());
      }
    }
    return pipelineOptions;
  }

  public DataflowPipelineJob runPipeline(ImportOptions pipelineOptions) throws IOException {
    return (DataflowPipelineJob) ImportJob.runPipeline(pipelineOptions);
  }

  private String waitForJobToRun(DataflowPipelineJob pipelineResult)
      throws RuntimeException, InterruptedException {
    // TODO: add timeout
    while (true) {
      State state = pipelineResult.getState();
      if (state.isTerminal()) {
        String dataflowDashboardUrl =
            String.format(
                "https://console.cloud.google.com/dataflow/jobsDetail/locations/%s/jobs/%s",
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
