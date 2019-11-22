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

import static feast.core.util.PipelineUtil.detectClassPathResourcesToStage;

import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import com.google.common.base.Strings;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Printer;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.StoreProto.Store;
import feast.core.config.FeastProperties.MetricsProperties;
import feast.core.exception.JobExecutionException;
import feast.core.job.JobManager;
import feast.core.job.Runner;
import feast.core.model.FeatureSet;
import feast.core.model.JobInfo;
import feast.core.util.TypeConversion;
import feast.ingestion.ImportJob;
import feast.ingestion.options.ImportOptions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
  private final Map<String, String> defaultOptions;
  private final MetricsProperties metrics;

  public DataflowJobManager(
      Dataflow dataflow, Map<String, String> defaultOptions, MetricsProperties metricsProperties) {
    this.defaultOptions = defaultOptions;
    this.dataflow = dataflow;
    this.metrics = metricsProperties;
    this.projectId = defaultOptions.get("project");
    this.location = defaultOptions.get("region");
  }

  @Override
  public Runner getRunnerType() {
    return RUNNER_TYPE;
  }

  @Override
  public String startJob(String name, List<FeatureSetSpec> featureSets, Store sink) {
    return submitDataflowJob(name, featureSets, sink, false);
  }

  /**
   * Update an existing Dataflow job.
   *
   * @param jobInfo jobInfo of target job to change
   * @return Dataflow-specific job id
   */
  @Override
  public String updateJob(JobInfo jobInfo) {
    try {
      List<FeatureSetSpec> featureSetSpecs = new ArrayList<>();
      for (FeatureSet featureSet : jobInfo.getFeatureSets()) {
        featureSetSpecs.add(featureSet.toProto());
      }
      return submitDataflowJob(
          jobInfo.getId(), featureSetSpecs, jobInfo.getStore().toProto(), true);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(String.format("Unable to update job %s", jobInfo.getId()), e);
    }
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

  private String submitDataflowJob(
      String jobName, List<FeatureSetSpec> featureSets, Store sink, boolean update) {
    try {
      ImportOptions pipelineOptions = getPipelineOptions(jobName, featureSets, sink, update);
      DataflowPipelineJob pipelineResult = runPipeline(pipelineOptions);
      String jobId = waitForJobToRun(pipelineResult);
      return jobId;
    } catch (Exception e) {
      log.error("Error submitting job", e);
      throw new JobExecutionException(String.format("Error running ingestion job: %s", e), e);
    }
  }

  private ImportOptions getPipelineOptions(
      String jobName, List<FeatureSetSpec> featureSets, Store sink, boolean update)
      throws IOException {
    String[] args = TypeConversion.convertMapToArgs(defaultOptions);
    ImportOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).as(ImportOptions.class);
    Printer printer = JsonFormat.printer();
    List<String> featureSetsJson = new ArrayList<>();
    for (FeatureSetSpec featureSet : featureSets) {
      featureSetsJson.add(printer.print(featureSet));
    }
    pipelineOptions.setFeatureSetSpecJson(featureSetsJson);
    pipelineOptions.setStoreJson(Collections.singletonList(printer.print(sink)));
    pipelineOptions.setProject(projectId);
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
