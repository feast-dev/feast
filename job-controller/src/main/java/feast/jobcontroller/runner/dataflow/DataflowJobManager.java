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
package feast.jobcontroller.runner.dataflow;

import static feast.ingestion.utils.SpecUtil.parseSourceJson;
import static feast.ingestion.utils.SpecUtil.parseStoreJsonList;
import static feast.jobcontroller.util.PipelineUtil.detectClassPathResourcesToStage;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.DataflowScopes;
import com.google.common.base.Strings;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import feast.common.models.FeatureSetReference;
import feast.ingestion.ImportJob;
import feast.ingestion.options.ImportOptions;
import feast.jobcontroller.config.FeastProperties.MetricsProperties;
import feast.jobcontroller.exception.JobExecutionException;
import feast.jobcontroller.model.Job;
import feast.jobcontroller.model.JobStatus;
import feast.jobcontroller.runner.JobManager;
import feast.jobcontroller.runner.Runner;
import feast.proto.core.IngestionJobProto;
import feast.proto.core.RunnerProto.DataflowRunnerConfigOptions;
import feast.proto.core.SourceProto;
import feast.proto.core.StoreProto;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.*;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.joda.time.DateTime;

@Slf4j
public class DataflowJobManager implements JobManager {

  private final Runner RUNNER_TYPE = Runner.DATAFLOW;

  private final String projectId;
  private final String location;
  private final Dataflow dataflow;
  private final DataflowRunnerConfig defaultOptions;
  private final MetricsProperties metrics;
  private final IngestionJobProto.SpecsStreamingUpdateConfig specsStreamingUpdateConfig;
  private final Map<String, String> jobSelector;

  public static DataflowJobManager of(
      DataflowRunnerConfigOptions runnerConfigOptions,
      MetricsProperties metricsProperties,
      IngestionJobProto.SpecsStreamingUpdateConfig specsStreamingUpdateConfig,
      Map<String, String> jobSelector) {

    Dataflow dataflow;
    try {
      dataflow =
          new Dataflow(
              GoogleNetHttpTransport.newTrustedTransport(),
              JacksonFactory.getDefaultInstance(),
              getGoogleCredential());
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("Security exception while connecting to Dataflow API", e);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to initialize DataflowJobManager", e);
    }

    return new DataflowJobManager(
        runnerConfigOptions, metricsProperties, specsStreamingUpdateConfig, jobSelector, dataflow);
  }

  DataflowJobManager(
      DataflowRunnerConfigOptions runnerConfigOptions,
      MetricsProperties metricsProperties,
      IngestionJobProto.SpecsStreamingUpdateConfig specsStreamingUpdateConfig,
      Map<String, String> jobSelector,
      Dataflow dataflow) {

    defaultOptions = new DataflowRunnerConfig(runnerConfigOptions);
    this.dataflow = dataflow;
    this.metrics = metricsProperties;
    this.projectId = defaultOptions.getProject();
    this.location = defaultOptions.getRegion();
    this.specsStreamingUpdateConfig = specsStreamingUpdateConfig;
    this.jobSelector = jobSelector;
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
              job.getSource(),
              new HashSet<>(job.getStores().values()),
              job.getLabels(),
              false);
      job.setExtId(extId);
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

  @Override
  public List<Job> listRunningJobs() {
    List<com.google.api.services.dataflow.model.Job> jobs;

    try {
      jobs =
          dataflow
              .projects()
              .locations()
              .jobs()
              .list(projectId, location)
              .setFilter("ACTIVE")
              .execute()
              .getJobs();
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Unable to retrieve list of jobs from dataflow: %s", e.getMessage()));
    }

    if (jobs == null) {
      return Collections.emptyList();
    }

    return jobs.stream()
        .map(
            dfJob -> {
              try {
                return dataflow
                    .projects()
                    .locations()
                    .jobs()
                    .get(projectId, location, dfJob.getId())
                    .setView("JOB_VIEW_ALL")
                    .execute();
              } catch (IOException e) {
                log.error(
                    "Job's detailed info {} couldn't be loaded from Dataflow: {}",
                    dfJob.getId(),
                    e.getMessage());
                return null;
              }
            })
        .filter(
            dfJob ->
                dfJob != null
                    && (dfJob.getLabels() != null || this.jobSelector.isEmpty())
                    && this.jobSelector.entrySet().stream()
                        .allMatch(
                            entry ->
                                dfJob
                                    .getLabels()
                                    .getOrDefault(entry.getKey(), "")
                                    .equals(entry.getValue())))
        .map(
            dfJob -> {
              Map<String, Object> options =
                  (Map<String, Object>)
                      dfJob.getEnvironment().getSdkPipelineOptions().get("options");

              List<StoreProto.Store> stores =
                  parseStoreJsonList((List<String>) options.get("storesJson"));
              SourceProto.Source source = parseSourceJson((String) options.get("sourceJson"));

              Job job =
                  Job.builder()
                      .setId((String) options.get("jobName"))
                      .setSource(source)
                      .setStores(
                          stores.stream()
                              .collect(Collectors.toMap(StoreProto.Store::getName, s -> s)))
                      .setLabels(
                          dfJob.getLabels() == null
                              ? new HashMap<>()
                              : new HashMap<>(dfJob.getLabels()))
                      .build();

              job.setExtId(dfJob.getId());
              job.setStatus(JobStatus.RUNNING);
              if (dfJob.getCreateTime() != null) {
                job.setCreated(DateTime.parse(dfJob.getCreateTime()).toDate());
              }

              return job;
            })
        .collect(Collectors.toList());
  }

  private String submitDataflowJob(
      String jobName,
      SourceProto.Source source,
      Set<StoreProto.Store> sinks,
      Map<String, String> labels,
      boolean update) {
    try {
      ImportOptions pipelineOptions = getPipelineOptions(jobName, source, sinks, labels, update);
      DataflowPipelineJob pipelineResult = runPipeline(pipelineOptions);
      String jobId = waitForJobToRun(pipelineResult);
      return jobId;
    } catch (Exception e) {
      log.error("Error submitting job", e);
      throw new JobExecutionException(String.format("Error running ingestion job: %s", e), e);
    }
  }

  private ImportOptions getPipelineOptions(
      String jobName,
      SourceProto.Source source,
      Set<StoreProto.Store> sinks,
      Map<String, String> labels,
      boolean update)
      throws IOException, IllegalAccessException {
    ImportOptions pipelineOptions =
        PipelineOptionsFactory.fromArgs(defaultOptions.toArgs()).as(ImportOptions.class);

    JsonFormat.Printer jsonPrinter = JsonFormat.printer();
    List<String> storesJson = new ArrayList<>();
    for (StoreProto.Store sink : sinks) {
      String print = jsonPrinter.print(sink);
      storesJson.add(print);
    }

    pipelineOptions.setSpecsStreamingUpdateConfigJson(
        jsonPrinter.print(specsStreamingUpdateConfig));
    pipelineOptions.setSourceJson(jsonPrinter.print(source));
    pipelineOptions.setStoresJson(storesJson);
    pipelineOptions.setProject(projectId);
    pipelineOptions.setDefaultFeastProject(FeatureSetReference.PROJECT_DEFAULT_NAME);
    pipelineOptions.setUpdate(update);
    pipelineOptions.setRunner(DataflowRunner.class);
    pipelineOptions.setJobName(jobName);
    pipelineOptions.setFilesToStage(
        detectClassPathResourcesToStage(DataflowRunner.class.getClassLoader()));

    // Merge common labels with job's labels
    Map<String, String> mergedLabels = new HashMap<>(defaultOptions.getLabels());
    labels.forEach(mergedLabels::put);
    pipelineOptions.setLabels(mergedLabels);

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
