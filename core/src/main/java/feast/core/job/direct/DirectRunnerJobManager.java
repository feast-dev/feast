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
import feast.core.util.TypeConversion;
import feast.specs.ImportJobSpecsProto.ImportJobSpecs;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DirectRunnerJobManager implements JobManager {

  private static final int SLEEP_MS = 10;
  private static final Pattern JOB_EXT_ID_PREFIX_REGEX = Pattern.compile(".*FeastImportJobId:.*");
  protected ImportJobDefaults defaults;

  public DirectRunnerJobManager(ImportJobDefaults importJobDefaults) {
    this.defaults = importJobDefaults;
  }

  @Override
  public String submitJob(ImportJobSpecs importJobSpecs, Path workspace) {
    ProcessBuilder pb = getProcessBuilder(importJobSpecs, workspace);

    log.info(String.format("Executing command: %s", String.join(" ", pb.command())));

    try {
      Process p = pb.start();
      return runProcess(p);
    } catch (Exception e) {
      log.error("Error submitting job", e);
      throw new JobExecutionException(String.format("Error running ingestion job: %s", e), e);
    }
  }

  @Override
  public void abortJob(String extId) {
    throw new UnsupportedOperationException("Unable to abort a job running in direct runner");
  }

  /**
   * Builds the command to execute the ingestion job
   *
   * @return configured ProcessBuilder
   */
  @VisibleForTesting
  public ProcessBuilder getProcessBuilder(ImportJobSpecs importJobSpecs, Path workspace) {
    Map<String, String> options =
        TypeConversion.convertJsonStringToMap(defaults.getImportJobOptions());
    List<String> commands = new ArrayList<>();
    commands.add("java");
    commands.add("-jar");
    commands.add(defaults.getExecutable());
    commands.add(option("jobName", importJobSpecs.getJobId()));
    commands.add(option("workspace", workspace.toString()));
    commands.add(option("runner", defaults.getRunner()));

    options.forEach((k, v) -> commands.add(option(k, v)));
    return new ProcessBuilder(commands);
  }

  /**
   * Run the given process and extract the job id from the output logs
   *
   * @param p Process
   * @return job id
   */
  @VisibleForTesting
  public String runProcess(Process p) {
    try (BufferedReader outputStream =
        new BufferedReader(new InputStreamReader(p.getInputStream()));
        BufferedReader errorsStream =
            new BufferedReader(new InputStreamReader(p.getErrorStream()))) {
      String extId = "";
      while (p.isAlive()) {
        while (outputStream.ready()) {
          String l = outputStream.readLine();
          System.out.println(l);
          if (JOB_EXT_ID_PREFIX_REGEX.matcher(l).matches()) {
            extId = l.split("FeastImportJobId:")[1];
          }
        }
        Thread.sleep(SLEEP_MS);
      }
      if (p.exitValue() > 0) {
        Optional<String> errorString = errorsStream.lines().reduce((l1, l2) -> l1 + '\n' + l2);
        throw new RuntimeException(String.format("Could not submit job: \n%s", errorString));
      }
      return extId;
    } catch (Exception e) {
      log.error("Error running ingestion job: ", e);
      throw new JobExecutionException(String.format("Error running ingestion job: %s", e), e);
    }
  }

  private String option(String key, String value) {
    return String.format("--%s=%s", key, value);
  }
}
