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
import feast.core.util.TypeConversion;
import feast.specs.ImportSpecProto.ImportSpec;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataflowJobManager implements JobManager {

  private static final int SLEEP_MS = 10;
  private static final Pattern JOB_EXT_ID_PREFIX_REGEX = Pattern.compile(".*FeastImportJobId:.*");

  private final String projectId;
  private final String location;
  private final Dataflow dataflow;
  private ImportJobDefaults defaults;

  public DataflowJobManager(
      Dataflow dataflow, String projectId, String location, ImportJobDefaults importJobDefaults) {
    checkNotNull(projectId);
    checkNotNull(location);
    this.projectId = projectId;
    this.location = location;
    this.dataflow = dataflow;
    this.defaults = importJobDefaults;
  }

  @Override
  public String submitJob(ImportSpec importSpec, String jobId) {
    ProcessBuilder pb = getProcessBuilder(importSpec, jobId);
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
   * @param importSpec
   * @param jobId
   * @return configured ProcessBuilder
   */
  private ProcessBuilder getProcessBuilder(ImportSpec importSpec, String jobId) {
    Map<String, String> options =
        TypeConversion.convertJsonStringToMap(defaults.getImportJobOptions());
    List<String> commands = new ArrayList<>();
    commands.add("java");
    commands.add("-jar");
    commands.add(defaults.getExecutable());
    commands.add(option("jobName", jobId));
    commands.add(option("runner", defaults.getRunner()));
    commands.add(
        option("importSpecBase64", Base64.getEncoder().encodeToString(importSpec.toByteArray())));
    commands.add(option("coreApiUri", defaults.getCoreApiUri()));
    commands.add(option("errorsStoreType", defaults.getErrorsStoreType()));
    commands.add(option("errorsStoreOptions", defaults.getErrorsStoreOptions()));

    options.forEach((k, v) -> commands.add(option(k, v)));
    return new ProcessBuilder(commands);
  }

  private String option(String key, String value) {
    return String.format("--%s=%s", key, value);
  }

  /**
   * Run the given process and extract the job id from the output logs
   *
   * @param p Process
   * @return job id
   */
  private String runProcess(Process p) {
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
}
