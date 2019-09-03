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
import feast.core.config.ImportJobDefaults;
import feast.core.job.JobManager;
import feast.core.model.JobInfo;
import java.io.IOException;
import java.nio.file.Path;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DirectRunnerJobManager implements JobManager {

  protected ImportJobDefaults defaults;
  private final DirectJobRegistry jobs;

  public DirectRunnerJobManager(ImportJobDefaults importJobDefaults, DirectJobRegistry jobs) {
    this.defaults = importJobDefaults;
    this.jobs = jobs;
  }

  /**
   * Start a direct runner job, that retrieves specs configuration from the given workspace.
   *
   * @param name job name
   * @param workspace containing specifications for running the job
   */
  @Override
  public String startJob(String name, Path workspace) {
    return "";
//    String[] args = TypeConversion.convertJsonStringToArgs(defaults.getImportJobOptions());
//
//    ImportJobPipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args)
//        .as(ImportJobPipelineOptions.class);
//    pipelineOptions.setRunner(DirectRunner.class);
//    pipelineOptions.setWorkspace(workspace.toUri().toString());
//    pipelineOptions
//        .setImportJobSpecUri(workspace.resolve("importJobSpecs.yaml").toUri().toString());
////    pipelineOptions
////        .setImportJobSpecUri(workspace.resolve("importJobSpecs.yaml").toAbsolutePath().toString());
//    pipelineOptions.setBlockOnRun(false);
//
//    try {
//      PipelineResult pipelineResult = runPipeline(pipelineOptions);
//      DirectJob directJob = new DirectJob(name, pipelineResult);
//      jobs.add(directJob);
//      return name;
//    } catch (Exception e) {
//      log.error("Error submitting job", e);
//      throw new JobExecutionException(String.format("Error running ingestion job: %s", e), e);
//    }
  }

  /**
   * Unsupported.
   */
  @Override
  public String updateJob(JobInfo jobInfo, Path workspace) {
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

//  public PipelineResult runPipeline(ImportJobPipelineOptions pipelineOptions)
//      throws IOException, URISyntaxException {
//    return ImportJob.runPipeline(pipelineOptions);
//  }
}
