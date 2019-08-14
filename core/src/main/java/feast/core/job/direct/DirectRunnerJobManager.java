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

import com.google.common.annotations.VisibleForTesting;
import feast.core.config.ImportJobDefaults;
import feast.core.exception.JobExecutionException;
import feast.core.job.JobManager;
import feast.core.model.JobInfo;
import feast.core.util.TypeConversion;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.Executor;

@Slf4j
public class DirectRunnerJobManager implements JobManager {

  protected ImportJobDefaults defaults;
  private final DirectJobRegistry jobs;

  public DirectRunnerJobManager(ImportJobDefaults importJobDefaults, DirectJobRegistry jobs) {
    this.defaults = importJobDefaults;
    this.jobs = jobs;
  }

  @Override
  public String startJob(String name, Path workspace) {
    CommandLine cmdLine = getCommandLine(name, workspace);

    log.info(String.format("Executing command: %s", String.join(" ", cmdLine.toString())));
    Executor executor = new DefaultExecutor();
    try {
      DirectJob directJob = runProcess(name, cmdLine, executor);
      jobs.add(directJob);
      return name;
    } catch (Exception e) {
      log.error("Error submitting job", e);
      throw new JobExecutionException(String.format("Error running ingestion job: %s", e), e);
    }
  }

  //TODO: update job when new features added to existing entity
  @Override
  public String updateJob(JobInfo jobInfo, Path workspace) {
    return null;
  }

  @Override
  public void abortJob(String extId) {
    DirectJob job = jobs.get(extId);
    job.getWatchdog().destroyProcess();
    jobs.remove(extId);
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
   * Run the given process
   * @param jobId job id of the job to run
   * @param cmdLine CommandLine object to execute
   * @param exec executor
   * @return
   * @throws IOException
   * @throws InterruptedException
   */
  @VisibleForTesting
  public DirectJob runProcess(String jobId, CommandLine cmdLine, Executor exec)
      throws IOException {
    DefaultExecuteResultHandler resultHandler = new DefaultExecuteResultHandler();
    ExecuteWatchdog watchdog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);

    exec.setWatchdog(watchdog);
    exec.execute(cmdLine, resultHandler);
    return new DirectJob(jobId, watchdog, resultHandler);
  }

  private String option(String key, String value) {
    return String.format("--%s=%s", key, value);
  }
}
