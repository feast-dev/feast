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

package feast.core.job.direct;

import com.google.common.base.Strings;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Printer;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.StoreProto;
import feast.core.exception.JobExecutionException;
import feast.core.job.JobManager;
import feast.core.job.Runner;
import feast.core.model.JobInfo;
import feast.core.util.TypeConversion;
import feast.ingestion.ImportJob;
import feast.ingestion.options.ImportJobPipelineOptions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

@Slf4j
public class DirectRunnerJobManager implements JobManager {

  private final Runner RUNNER_TYPE = Runner.DIRECT;

  protected Map<String, String> defaultOptions;
  private final DirectJobRegistry jobs;


  public DirectRunnerJobManager(Map<String, String> defaultOptions, DirectJobRegistry jobs) {
    this.defaultOptions = defaultOptions;
    this.jobs = jobs;
  }

  @Override
  public Runner getRunnerType() {
    return RUNNER_TYPE;
  }

  /**
   * Start a direct runner job.
   *
   * @param name of job to run
   * @param featureSetSpecs list of specs for featureSets to be populated by the job
   * @param sinkSpec Store to sink features to
   */
  @Override
  public String startJob(String name, List<FeatureSetSpec> featureSetSpecs, StoreProto.Store sinkSpec) {
    try {
      ImportJobPipelineOptions pipelineOptions = getPipelineOptions(featureSetSpecs, sinkSpec);
      PipelineResult pipelineResult = runPipeline(pipelineOptions);
      DirectJob directJob = new DirectJob(name, pipelineResult);
      jobs.add(directJob);
      return name;
    } catch (Exception e) {
      log.error("Error submitting job", e);
      throw new JobExecutionException(String.format("Error running ingestion job: %s", e), e);
    }
  }

  private ImportJobPipelineOptions getPipelineOptions(List<FeatureSetSpec> featureSetSpecs,
      StoreProto.Store sink)
      throws InvalidProtocolBufferException {
    String[] args = TypeConversion.convertMapToArgs(defaultOptions);
    ImportJobPipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args)
        .as(ImportJobPipelineOptions.class);
    Printer printer = JsonFormat.printer();
    List<String> featureSetsJson = new ArrayList<>();
    for (FeatureSetSpec featureSetSpec : featureSetSpecs) {
      featureSetsJson.add(printer.print(featureSetSpec));
    }
    pipelineOptions.setFeatureSetSpecJson(featureSetsJson);
    pipelineOptions.setStoreJson(Collections.singletonList(printer.print(sink)));
    pipelineOptions.setRunner(DirectRunner.class);
    pipelineOptions.setBlockOnRun(false);
    return pipelineOptions;
  }

  /**
   * Unsupported.
   */
  @Override
  public String updateJob(JobInfo jobInfo) {
    throw new UnsupportedOperationException(
        "DirectRunner does not support job updates. To make changes to the worker, stop the existing job and rerun ingestion.");
  }

  /**
   * Abort the direct runner job with the given id, then remove it from the direct jobs registry.
   *
   * @param extId runner specific job id.
   */
  @Override
  public void abortJob(String extId) {
    DirectJob job = jobs.get(extId);
    try {
      job.abort();
    } catch (IOException e) {
      throw new RuntimeException(
          Strings.lenientFormat("Unable to abort DirectRunner job %s", extId), e);
    }
    jobs.remove(extId);
  }

  public PipelineResult runPipeline(ImportJobPipelineOptions pipelineOptions)
      throws IOException {
    return ImportJob.runPipeline(pipelineOptions);
  }
}
