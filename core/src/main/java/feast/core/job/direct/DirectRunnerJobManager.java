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
package feast.core.job.direct;

import static feast.core.util.StreamUtil.wrapException;

import com.google.common.base.Strings;
import com.google.protobuf.util.JsonFormat;
import feast.core.config.FeastProperties.MetricsProperties;
import feast.core.exception.JobExecutionException;
import feast.core.job.JobManager;
import feast.core.job.Runner;
import feast.core.model.*;
import feast.ingestion.ImportJob;
import feast.ingestion.options.ImportOptions;
import feast.proto.core.IngestionJobProto;
import feast.proto.core.RunnerProto.DirectRunnerConfigOptions;
import feast.proto.core.SourceProto;
import feast.proto.core.StoreProto;
import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

@Slf4j
public class DirectRunnerJobManager implements JobManager {

  private final Runner RUNNER_TYPE = Runner.DIRECT;

  private DirectRunnerConfig defaultOptions;
  private final DirectJobRegistry jobs;
  private MetricsProperties metrics;
  private final IngestionJobProto.SpecsStreamingUpdateConfig specsStreamingUpdateConfig;

  public DirectRunnerJobManager(
      DirectRunnerConfigOptions directRunnerConfigOptions,
      DirectJobRegistry jobs,
      MetricsProperties metricsProperties,
      IngestionJobProto.SpecsStreamingUpdateConfig specsStreamingUpdateConfig) {
    this.defaultOptions = new DirectRunnerConfig(directRunnerConfigOptions);
    this.jobs = jobs;
    this.metrics = metricsProperties;
    this.specsStreamingUpdateConfig = specsStreamingUpdateConfig;
  }

  @Override
  public Runner getRunnerType() {
    return RUNNER_TYPE;
  }

  /**
   * Start a direct runner job.
   *
   * @param job Job to start
   */
  @Override
  public Job startJob(Job job) {
    try {
      ImportOptions pipelineOptions =
          getPipelineOptions(
              job.getId(),
              job.getSource().toProto(),
              job.getStores().stream()
                  .map(wrapException(Store::toProto))
                  .collect(Collectors.toSet()));
      PipelineResult pipelineResult = runPipeline(pipelineOptions);
      DirectJob directJob = new DirectJob(job.getId(), pipelineResult);
      jobs.add(directJob);
      job.setExtId(job.getId());
      job.setStatus(JobStatus.RUNNING);
      return job;
    } catch (Exception e) {
      log.error("Error submitting job", e);
      throw new JobExecutionException(String.format("Error running ingestion job: %s", e), e);
    }
  }

  private ImportOptions getPipelineOptions(
      String jobName, SourceProto.Source source, Set<StoreProto.Store> sinks)
      throws IOException, IllegalAccessException {
    ImportOptions pipelineOptions =
        PipelineOptionsFactory.fromArgs(defaultOptions.toArgs()).as(ImportOptions.class);

    JsonFormat.Printer printer = JsonFormat.printer();
    pipelineOptions.setSpecsStreamingUpdateConfigJson(printer.print(specsStreamingUpdateConfig));
    pipelineOptions.setSourceJson(printer.print(source));
    pipelineOptions.setJobName(jobName);
    pipelineOptions.setStoresJson(
        sinks.stream().map(wrapException(printer::print)).collect(Collectors.toList()));
    pipelineOptions.setRunner(DirectRunner.class);
    pipelineOptions.setDefaultFeastProject(Project.DEFAULT_NAME);
    pipelineOptions.setProject(""); // set to default value to satisfy validation
    if (metrics.isEnabled()) {
      pipelineOptions.setMetricsExporterType(metrics.getType());
      if (metrics.getType().equals("statsd")) {
        pipelineOptions.setStatsdHost(metrics.getHost());
        pipelineOptions.setStatsdPort(metrics.getPort());
      }
    }
    pipelineOptions.setBlockOnRun(false);
    return pipelineOptions;
  }

  /**
   * Stops an existing job and restarts a new job in its place as a proxy for job updates. Note that
   * since we do not maintain a consumer group across the two jobs and the old job is not drained,
   * some data may be lost.
   *
   * <p>As a rule of thumb, direct jobs in feast should only be used for testing.
   *
   * @param job job of target job to change
   * @return jobId of the job
   */
  @Override
  public Job updateJob(Job job) {
    try {
      return startJob(abortJob(job));
    } catch (JobExecutionException e) {
      throw new JobExecutionException(String.format("Error running ingestion job: %s", e), e);
    }
  }

  /**
   * Abort the direct runner job,removing it from the direct jobs registry.
   *
   * @param job to abort.
   * @return The aborted Job
   */
  @Override
  public Job abortJob(Job job) {
    DirectJob directJob = jobs.get(job.getExtId());
    if (directJob != null) {
      try {
        directJob.abort();
      } catch (IOException e) {
        throw new RuntimeException(
            Strings.lenientFormat("Unable to abort DirectRunner job %s", job.getExtId(), e));
      }
      jobs.remove(job.getExtId());
    }

    job.setStatus(JobStatus.ABORTING);
    return job;
  }

  public PipelineResult runPipeline(ImportOptions pipelineOptions) throws IOException {
    return ImportJob.runPipeline(pipelineOptions);
  }

  /**
   * Restart a direct runner job. Note that some data will be temporarily lost during when
   * restarting running direct runner jobs. See {#link {@link #updateJob(Job)} for more info.
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
      // the effect of restarting the job.
      return this.updateJob(job);
    }
  }

  /**
   * Gets the state of the direct runner job. Direct runner jobs only have 2 states: RUNNING and
   * ABORTED.
   *
   * @param job Job of the desired job.
   * @return JobStatus of the job.
   */
  @Override
  public JobStatus getJobStatus(Job job) {
    DirectJob directJob = jobs.get(job.getId());
    if (directJob == null) {
      return JobStatus.ABORTED;
    }
    return DirectJobStateMapper.map(directJob.getPipelineResult().getState());
  }
}
