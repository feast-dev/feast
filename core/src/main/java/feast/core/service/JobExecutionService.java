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

package feast.core.service;

import feast.core.JobServiceProto.JobServiceTypes.SubmitImportJobResponse;
import feast.core.config.ImportJobDefaults;
import feast.core.dao.JobInfoRepository;
import feast.core.exception.JobExecutionException;
import feast.core.log.Action;
import feast.core.log.AuditLogger;
import feast.core.log.Resource;
import feast.core.model.JobInfo;
import feast.core.model.JobStatus;
import feast.core.util.TypeConversion;
import feast.specs.ImportSpecProto.ImportSpec;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class JobExecutionService {

  public static final String JOB_PREFIX_DEFAULT = "feastimport";
  private static final int SLEEP_MS = 10;
  private static final Pattern JOB_EXT_ID_PREFIX_REGEX = Pattern.compile(".*FeastImportJobId:.*");
  private JobInfoRepository jobInfoRepository;
  private ImportJobDefaults defaults;

  @Autowired
  public JobExecutionService(JobInfoRepository jobInfoRepository, ImportJobDefaults defaults) {
    this.jobInfoRepository = jobInfoRepository;
    this.defaults = defaults;
  }

  /**
   * Submits a job defined by the importSpec to the runner and writes details about the job to the
   * core database.
   *
   * @param importSpec job import spec
   * @param jobPrefix prefix for job name
   * @return response with feast-internal job id
   */
  public SubmitImportJobResponse submitJob(ImportSpec importSpec, String jobPrefix) {
    String dateSuffix = String.valueOf(Instant.now().toEpochMilli());
    String jobId = jobPrefix.isEmpty() ? JOB_PREFIX_DEFAULT + dateSuffix : jobPrefix + dateSuffix;
    ProcessBuilder pb = getProcessBuilder(importSpec, jobId);
    log.info(String.format("Executing command: %s", String.join(" ", pb.command())));
    AuditLogger.log(
        Resource.JOB,
        jobId,
        Action.SUBMIT,
        "Building graph and submitting to %s",
        defaults.getRunner());
    try {
      JobInfo jobInfo = new JobInfo(jobId, "", defaults.getRunner(), importSpec, JobStatus.PENDING);
      jobInfoRepository.saveAndFlush(jobInfo);
      Process p = pb.start();
      String jobExtId = runProcess(p);
      if (jobExtId.isEmpty()) {
        throw new RuntimeException(
            String.format("Could not submit job: \n%s", "unable to retrieve job external id"));
      }
      updateJobExtId(jobId, jobExtId);
      AuditLogger.log(
          Resource.JOB,
          jobId,
          Action.STATUS_CHANGE,
          "Job submitted to runner %s with runner id %s.",
          defaults.getRunner(),
          jobExtId);
      return SubmitImportJobResponse.newBuilder().setJobId(jobId).build();
    } catch (Exception e) {
      updateJobStatus(jobId, JobStatus.ERROR);
      AuditLogger.log(
          Resource.JOB,
          jobId,
          Action.STATUS_CHANGE,
          "Job failed to be submitted to runner %s. Job status changed to ERROR.",
          defaults.getRunner());
      throw new JobExecutionException(String.format("Error running ingestion job: %s", e), e);
    }
  }

  /**
   * Update a given job's status
   */
  public void updateJobStatus(String jobId, JobStatus status) {
    Optional<JobInfo> jobRecordOptional = jobInfoRepository.findById(jobId);
    if (jobRecordOptional.isPresent()) {
      JobInfo jobRecord = jobRecordOptional.get();
      jobRecord.setStatus(status);
      jobInfoRepository.saveAndFlush(jobRecord);
    }
  }

  /**
   * Update a given job's external id
   */
  public void updateJobExtId(String jobId, String jobExtId) {
    Optional<JobInfo> jobRecordOptional = jobInfoRepository.findById(jobId);
    if (jobRecordOptional.isPresent()) {
      JobInfo jobRecord = jobRecordOptional.get();
      jobRecord.setExtId(jobExtId);
      jobInfoRepository.saveAndFlush(jobRecord);
    }
  }

  /**
   * Builds the command to execute the ingestion job
   *
   * @return configured ProcessBuilder
   */
  public ProcessBuilder getProcessBuilder(ImportSpec importSpec, String jobId) {
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
}
