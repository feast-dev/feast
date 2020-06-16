/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
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
package feast.core.job.databricks;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import feast.core.exception.JobExecutionException;
import feast.core.job.JobManager;
import feast.core.job.Runner;
import feast.core.model.*;
import feast.databricks.types.*;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.RunnerProto.DatabricksRunnerConfigOptions;
import feast.proto.core.StoreProto;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpException;
import org.apache.http.HttpStatus;

@Slf4j
public class DatabricksJobManager implements JobManager {
  private static final String SPARK_INGESTION_CLASS = "feast.spark.ingestion.SparkIngestion";

  private static final Runner RUNNER_TYPE = Runner.DATABRICKS;

  private final String databricksHost;
  private final byte[] databricksToken;
  private final String jarFile;
  private final DatabricksRunnerConfigOptions.DatabricksNewClusterOptions newClusterConfigOptions;
  private final HttpClient httpClient;
  private static final ObjectMapper mapper = ObjectMapperFactory.createObjectMapper();
  private final int maxRetries;

  public DatabricksJobManager(
      DatabricksRunnerConfigOptions runnerConfigOptions, HttpClient httpClient) {

    this.databricksHost = runnerConfigOptions.getHost();
    this.databricksToken = runnerConfigOptions.getToken().getBytes(StandardCharsets.UTF_8);
    this.httpClient = httpClient;
    this.newClusterConfigOptions = runnerConfigOptions.getNewCluster();
    this.jarFile = runnerConfigOptions.getJarFile();
    this.maxRetries = runnerConfigOptions.getMaxRetries();
  }

  @Override
  public Runner getRunnerType() {
    return RUNNER_TYPE;
  }

  @Override
  public Job startJob(Job job) {
    String jobId = job.getId();

    long databricksJobId = createDatabricksJob(job);

    log.info("Created job for Feast job {} (Databricks JobId {})", jobId, databricksJobId);

    long databricksRunId = runDatabricksJob(databricksJobId);

    log.info("Setting job status for job {} (Databricks RunId {})", jobId, databricksRunId);
    job.setExtId(Long.toString(databricksRunId));

    log.info(
        "Waiting for job {} to start (Databricks JobId {} RunId {})",
        jobId,
        databricksJobId,
        databricksRunId);
    waitForJobToStart(job);

    return job;
  }

  /**
   * Update an existing Databricks job.
   *
   * @param job job of target job to change
   * @return Databricks-specific job id
   */
  @Override
  public Job updateJob(Job job) {
    return restartJob(job);
  }

  @Override
  public void abortJob(String runId) {
    log.info("Aborting job (Databricks RunId {})", runId);

    try {
      RunsCancelRequest runsCancelRequest =
          RunsCancelRequest.builder().setRunId(Long.parseLong(runId)).build();
      String body = mapper.writeValueAsString(runsCancelRequest);

      HttpRequest.Builder request =
          HttpRequest.newBuilder()
              .uri(getAbortUri())
              .POST(HttpRequest.BodyPublishers.ofString(body));

      sendDatabricksRequest(request);

    } catch (IOException | InterruptedException | HttpException e) {
      log.error(
          "Unable to abort databricks job with run id : {}\ncause: {}",
          runId,
          e.getMessage(),
          e.getCause());
      throw new JobExecutionException(
          String.format(
              "Unable to abort databricks job with run id : %s \nmessage: %s\ncause: %s",
              runId, e.getMessage(), e.getCause()),
          e);
    }
  }

  private URI getAbortUri() {
    return URI.create(String.format("%s/api/2.0/jobs/runs/cancel", databricksHost));
  }

  @Override
  public Job restartJob(Job job) {
    abortJob(job.getExtId());
    waitForJobToTerminate(job);
    return startJob(job);
  }

  @Override
  public JobStatus getJobStatus(Job job) {
    log.info("Getting job status for job {} (Databricks RunId {})", job.getId(), job.getExtId());
    HttpRequest.Builder request = HttpRequest.newBuilder().uri(getJobUri(job));
    try {
      HttpResponse<String> response = sendDatabricksRequest(request);

      RunsGetResponse runsGetResponse = mapper.readValue(response.body(), RunsGetResponse.class);
      RunState runState = runsGetResponse.getState();
      String lifeCycleState = runState.getLifeCycleState().toString();

      Optional<JobStatus> status =
          runState
              .getResultState()
              .map(
                  runResultState ->
                      DatabricksJobStateMapper.mapJobState(
                          String.format("%s_%s", lifeCycleState, runResultState.toString())))
              .orElseGet(() -> DatabricksJobStateMapper.mapJobState(lifeCycleState));

      log.info(
          "Databricks job state for job {} (Databricks RunId {}) is {} (mapped to {})",
          job.getId(),
          job.getExtId(),
          runState,
          status);

      if (status.isEmpty()) {
        log.error("Unknown status: %s", runState);
        return JobStatus.UNKNOWN;
      }

      return status.get();

    } catch (IOException | InterruptedException | HttpException ex) {
      log.error("Unable to retrieve status of a databricks run with id " + job.getExtId(), ex);
      return JobStatus.UNKNOWN;
    }
  }

  private URI getJobUri(Job job) {
    return URI.create(
        String.format("%s/api/2.0/jobs/runs/get?run_id=%s", databricksHost, job.getExtId()));
  }

  private HttpResponse<String> sendDatabricksRequest(HttpRequest.Builder builder)
      throws IOException, InterruptedException, HttpException {

    String authorizationHeader =
        String.format("%s %s", "Bearer", new String(databricksToken, StandardCharsets.UTF_8));

    HttpRequest request = builder.header("Authorization", authorizationHeader).build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != HttpStatus.SC_OK) {
      throw new HttpException(
          String.format(
              "Databricks returned with unexpected code: %s -- %s",
              response.statusCode(), response.body()));
    }
    return response;
  }

  private long createDatabricksJob(Job job) {
    log.info("Starting job id {} for sink {}", job.getId(), job.getSinkName());

    String jobName = String.format("Feast ingestion job %s", job.getId());
    String defaultFeastProject = Project.DEFAULT_NAME;
    List<FeatureSetProto.FeatureSetSpec> featureSetSpecsProtos = new ArrayList<>();
    StoreProto.Store store;
    try {
      for (FeatureSet featureSet : job.getFeatureSets()) {
        featureSetSpecsProtos.add(featureSet.toProto().getSpec());
      }
      store = job.getStore().toProto();
    } catch (InvalidProtocolBufferException e) {
      log.error("ERROR", e);
      log.error(e.getMessage(), e);
      throw new IllegalArgumentException(
          String.format(
              "DatabricksJobManager failed to START job with id '%s' because the job"
                  + "has an invalid spec. Please check the FeatureSet, Source and Store specs. Actual error message: %s",
              job.getId(), e.getMessage()),
          e);
    }

    String storesJson = toJsonLine(store);
    String featureSetsJson = toJsonLines(featureSetSpecsProtos);

    JobsCreateRequest createRequest =
        getJobRequest(
            jobName, Arrays.asList(job.getId(), defaultFeastProject, featureSetsJson, storesJson));

    try {
      String body = mapper.writeValueAsString(createRequest);

      HttpRequest.Builder request =
          HttpRequest.newBuilder()
              .uri(getJobsCreateUri())
              .POST(HttpRequest.BodyPublishers.ofString(body));

      HttpResponse<String> response = sendDatabricksRequest(request);
      JobsCreateResponse createResponse =
          mapper.readValue(response.body(), JobsCreateResponse.class);

      return createResponse.getJobId();

    } catch (IOException | InterruptedException | HttpException e) {
      log.error("Unable to run databricks job" + jobName, e);
      throw new JobExecutionException(
          String.format(
              "Unable to run databricks job : %s \nmessage: %s\ncause: %s",
              jobName, e, e.getCause()),
          e);
    }
  }

  private URI getJobsCreateUri() {
    return URI.create(String.format("%s/api/2.0/jobs/create", databricksHost));
  }

  private long runDatabricksJob(long databricksJobId) {
    log.info("Starting run for Databricks JobId {}", databricksJobId);

    RunNowRequest runNowRequest = getRunNowRequest(databricksJobId);

    try {
      String body = mapper.writeValueAsString(runNowRequest);

      HttpRequest.Builder request =
          HttpRequest.newBuilder()
              .uri(getRunNowUri())
              .POST(HttpRequest.BodyPublishers.ofString(body));

      HttpResponse<String> response = sendDatabricksRequest(request);

      RunNowResponse runNowResponse = mapper.readValue(response.body(), RunNowResponse.class);
      return runNowResponse.getRunId();
    } catch (Exception e) {
      log.error("Unable to run databricks job with id " + databricksJobId, e);
      throw new JobExecutionException(
          String.format(
              "Unable to run databricks job with id : %s \nmessage: %s\ncause: %s",
              databricksJobId, e.getMessage(), e.getCause()),
          e);
    }
  }

  private URI getRunNowUri() {
    return URI.create(String.format("%s/api/2.0/jobs/run-now", databricksHost));
  }

  private RunNowRequest getRunNowRequest(long databricksJobId) {
    RunNowRequest runNowRequest = RunNowRequest.builder().setJobId(databricksJobId).build();

    return runNowRequest;
  }

  private JobsCreateRequest getJobRequest(String jobName, List<String> params) {
    log.info("sparkConf: [%s]", newClusterConfigOptions.getSparkConf());
    Arrays.stream(newClusterConfigOptions.getSparkConf().strip().split("\n"))
        .forEach(s -> log.info("sparkConf line: [%s]", s));
    Arrays.stream(newClusterConfigOptions.getSparkConf().strip().split("\n"))
        .map(s -> s.strip().split("\\s+", 2))
        .forEach(s -> log.info("sparkConf line: [%s] + %d", s[0], s.length));
    log.info(newClusterConfigOptions.getSparkConf().strip());
    Map<String, String> sparkConf =
        Arrays.stream(newClusterConfigOptions.getSparkConf().strip().split("\n"))
            .map(s -> s.strip().split("\\s+", 2))
            .collect(Collectors.toMap(s -> s[0], s -> s[1]));

    NewCluster newCluster =
        NewCluster.builder()
            .setNumWorkers(newClusterConfigOptions.getNumWorkers())
            .setNodeTypeId(newClusterConfigOptions.getNodeTypeId())
            .setSparkVersion(newClusterConfigOptions.getSparkVersion())
            .setSparkConf(sparkConf)
            .build();

    Library library = Library.builder().setJar(jarFile).build();
    List<Library> libraries = Collections.singletonList(library);

    SparkJarTask sparkJarTask =
        SparkJarTask.builder()
            .setMainClassName(SPARK_INGESTION_CLASS)
            .setParameters(params)
            .build();

    JobsCreateRequest createRequest =
        JobsCreateRequest.builder()
            .setLibraries(libraries)
            .setMaxRetries(maxRetries)
            .setName(jobName)
            .setNewCluster(newCluster)
            .setSparkJarTask(sparkJarTask)
            .build();

    return createRequest;
  }

  private JobStatus waitForJobToStart(Job job) {
    JobStatus jobStatus = waitForJobStatus(job, Set.of(JobStatus.RUNNING));

    if (jobStatus.isTerminal()) {
      throw new JobExecutionException(
          String.format(
              "Error running ingestion job: Failed to submit Databricks job %s: status %s",
              job.getExtId(), jobStatus.toString()));
    }
    return jobStatus;
  }

  private JobStatus waitForJobToTerminate(Job job) {
    return waitForJobStatus(job, JobStatus.getTerminalStates());
  }

  private JobStatus waitForJobStatus(Job job, Set<JobStatus> statusSet) {

    while (true) {
      JobStatus jobStatus = getJobStatus(job);
      if (jobStatus.isTerminal() || statusSet.contains(jobStatus)) {
        return jobStatus;
      }
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        // no action
      }
    }
  }

  private static <T extends MessageOrBuilder> String toJsonLines(Collection<T> items) {
    return items.stream().map(DatabricksJobManager::toJsonLine).collect(Collectors.joining("\n"));
  }

  private static <T extends MessageOrBuilder> String toJsonLine(T item) {
    try {
      return JsonFormat.printer()
          .omittingInsignificantWhitespace()
          .printingEnumsAsInts()
          .print(item);
    } catch (InvalidProtocolBufferException e) {
      log.error("ERROR", e);
      throw new RuntimeException(e);
    }
  }
}
