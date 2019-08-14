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
import static java.lang.Integer.max;

import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import feast.core.config.ImportJobDefaults;
import feast.core.exception.JobExecutionException;
import feast.core.job.JobManager;
import feast.core.job.direct.DirectJob;
import feast.core.job.direct.DirectRunnerJobManager;
import feast.core.model.EntityInfo;
import feast.core.model.FeatureInfo;
import feast.core.model.JobInfo;
import feast.core.model.StorageInfo;
import feast.core.util.TypeConversion;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import java.util.Map;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.PumpStreamHandler;

@Slf4j
public class DataflowJobManager implements JobManager {

  private final String projectId;
  private final String location;
  private final Dataflow dataflow;
  private final ImportJobDefaults defaults;

  private static final Pattern JOB_EXT_ID_PREFIX_REGEX = Pattern.compile(".*FeastImportJobId:.*");

  public DataflowJobManager(
      Dataflow dataflow, String projectId, String location, ImportJobDefaults importJobDefaults) {
    this.defaults = importJobDefaults;
    checkNotNull(projectId);
    checkNotNull(location);
    this.projectId = projectId;
    this.location = location;
    this.dataflow = dataflow;
  }

  @Override
  public String startJob(String name, Path workspace) {
    CommandLine cmdLine = getCommandLine(name, workspace);

    log.info(String.format("Executing command: %s", String.join(" ", cmdLine.toString())));
    Executor executor = new DefaultExecutor();
    try {
      String jobId = runProcess(name, cmdLine, executor);
      return jobId;
    } catch (Exception e) {
      log.error("Error submitting job", e);
      throw new JobExecutionException(String.format("Error running ingestion job: %s", e), e);
    }
  }

  @Override
  public String updateJob(JobInfo jobInfo, Path workspace) {
    return null;
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

  /**
   * Builds the command to execute the ingestion job
   *
   * @return configured ProcessBuilder
   */
  @VisibleForTesting
  public CommandLine getCommandLine(String name, Path workspace) {
    Map<String, String> options =
        TypeConversion.convertJsonStringToMap(defaults.getImportJobOptions());
    CommandLine cmdLine = new CommandLine("java");
    cmdLine.addArgument("-jar");
    cmdLine.addArgument(defaults.getExecutable());
    cmdLine.addArgument(option("workspace", workspace.toUri().toString()));
    cmdLine.addArgument(option("jobName", name));
    cmdLine.addArgument(option("runner", defaults.getRunner()));

    options.forEach((k, v) -> cmdLine.addArgument(option(k, v)));
    return cmdLine;
  }

  /**
   * Run the given process and extract the job id from the output logs
   *
   * @param jobId of the job to run
   * @param cmdLine CommandLine object to execute
   * @param exec CommandLine executor
   * @return job ID specific to dataflow, to use with the dataflow API
   * @throws IOException
   */
  @VisibleForTesting
  public String runProcess(String jobId, CommandLine cmdLine, Executor exec) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream);
    exec.setStreamHandler(streamHandler);
    int exitCode = exec.execute(cmdLine);
    String output = outputStream.toString();
    if (exitCode != 0) {
      log.error("Error running ingestion job {}", jobId);
      log.error(output);
      throw new JobExecutionException(String.format("Error running ingestion job %s", jobId));
    }
    String dfJobId = getJobId(output);
    if (dfJobId.isEmpty()) {
      log.error(output);
      throw new JobExecutionException(
          String.format("Error running ingestion job %s: extId not returned", jobId));
    }
    return dfJobId ;
  }

  private String getJobId(String output) {
    output = output.trim();
    int lastIndex = max(0, output.lastIndexOf("\n"));
    String lastLine = output.substring(lastIndex);
    if (JOB_EXT_ID_PREFIX_REGEX.matcher(lastLine).matches()) {
      return lastLine.split("FeastImportJobId:")[1];
    }
    return "";
  }

  private String option(String key, String value) {
    return String.format("--%s=%s", key, value);
  }
}
