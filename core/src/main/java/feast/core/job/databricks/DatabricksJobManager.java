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
import feast.core.config.FeastProperties.MetricsProperties;
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
  private final MetricsProperties metricsProperties;
  private final HttpClient httpClient;
  private static final ObjectMapper mapper = ObjectMapperFactory.createObjectMapper();
  private static final int maxRetries = -1;

  public DatabricksJobManager(
      DatabricksRunnerConfigOptions runnerConfigOptions,
      MetricsProperties metricsProperties,
      HttpClient httpClient) {

    this.databricksHost = runnerConfigOptions.getHost();
    this.databricksToken = runnerConfigOptions.getToken().getBytes(StandardCharsets.UTF_8);
    this.metricsProperties = metricsProperties;
    this.httpClient = httpClient;
    this.newClusterConfigOptions = runnerConfigOptions.getNewCluster();
    this.jarFile = runnerConfigOptions.getJarFile();
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

    try {
      RunsCancelRequest runsCancelRequest =
          RunsCancelRequest.builder().setRunId(Integer.parseInt(runId)).build();
      String body = mapper.writeValueAsString(runsCancelRequest);

      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(String.format("%s/api/2.0/jobs/runs/cancel/", databricksHost)))
              .header("Authorization", getAuthorizationHeader())
              .POST(HttpRequest.BodyPublishers.ofString(body))
              .build();

      HttpResponse<String> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    } catch (IOException | InterruptedException e) {
      log.error(
          "Unable to abort databricks job with run id : {}\ncause: {}", runId, e.getMessage());
      throw new JobExecutionException(
          String.format("Unable to abort databricks job with run id : %s\ncause: %s", runId, e), e);
    }
  }

  @Override
  public Job restartJob(Job job) {
    abortJob(job.getExtId());
    return startJob(job);
  }

  @Override
  public JobStatus getJobStatus(Job job) {
    log.info("Getting job status for job {} (Databricks RunId {})", job.getId(), job.getExtId());
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(
                URI.create(
                    String.format(
                        "%s/api/2.0/jobs/runs/get?run_id=%s", databricksHost, job.getExtId())))
            .header("Authorization", getAuthorizationHeader())
            .build();
    try {
      HttpResponse<String> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() == HttpStatus.SC_OK) {
        RunsGetResponse runsGetResponse = mapper.readValue(response.body(), RunsGetResponse.class);
        RunState runState = runsGetResponse.getState();
        String lifeCycleState = runState.getLifeCycleState().toString();

        log.info(
            "Databricks job state for job {} (Databricks RunId {}) is {}",
            job.getId(),
            job.getExtId(),
            runState);

        return runState
            .getResultState()
            .map(
                runResultState ->
                    DatabricksJobStateMapper.map(
                        String.format("%s_%s", lifeCycleState, runResultState.toString())))
            .orElseGet(() -> DatabricksJobStateMapper.map(lifeCycleState));

      } else {
        throw new HttpException(
            String.format("Databricks returned with unexpected code: %s", response.statusCode()));
      }
    } catch (IOException | InterruptedException | HttpException ex) {
      log.error(
          "Unable to retrieve status of a dabatricks run with id : {}\ncause: {}",
          job.getExtId(),
          ex.getMessage());
    }

    return JobStatus.UNKNOWN;
  }

  private String getAuthorizationHeader() {
    return String.format("%s %s", "Bearer", new String(databricksToken, StandardCharsets.UTF_8));
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

      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(String.format("%s/api/2.0/jobs/create", databricksHost)))
              .header("Authorization", getAuthorizationHeader())
              .POST(HttpRequest.BodyPublishers.ofString(body))
              .build();

      HttpResponse<String> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() == HttpStatus.SC_OK) {
        JobsCreateResponse createResponse =
            mapper.readValue(response.body(), JobsCreateResponse.class);

        return createResponse.getJobId();
      } else {
        throw new HttpException(
            String.format("Databricks returned with unexpected code: %s", response.statusCode()));
      }
    } catch (IOException | InterruptedException | HttpException e) {
      log.error("Unable to run databricks job : {}\ncause: {}", jobName, e.getMessage());
      throw new JobExecutionException(
          String.format("Unable to run databricks job : %s\ncause: %s", jobName, e), e);
    }
  }

  private long runDatabricksJob(long databricksJobId) {
    log.info("Starting run for Databricks JobId {}", databricksJobId);

    RunNowRequest runNowRequest = getRunNowRequest(databricksJobId);

    try {
      String body = mapper.writeValueAsString(runNowRequest);

      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(String.format("%s/api/2.0/jobs/run-now", databricksHost)))
              .header("Authorization", getAuthorizationHeader())
              .POST(HttpRequest.BodyPublishers.ofString(body))
              .build();

      HttpResponse<String> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() == HttpStatus.SC_OK) {
        RunNowResponse runNowResponse = mapper.readValue(response.body(), RunNowResponse.class);
        return runNowResponse.getRunId();
      } else {
        throw new HttpException(
            String.format("Databricks returned with unexpected code: %s", response.statusCode()));
      }
    } catch (Exception e) {
      log.error(
          "Unable to run databricks job with id : {}\ncause: {}", databricksJobId, e.getMessage());
      throw new JobExecutionException(
          String.format("Unable to run databricks job with id : %s\ncause: %s", databricksJobId, e),
          e);
    }
  }

  private RunNowRequest getRunNowRequest(long databricksJobId) {
    RunNowRequest runNowRequest = RunNowRequest.builder().setJobId(databricksJobId).build();

    return runNowRequest;
  }

  private JobsCreateRequest getJobRequest(String jobName, List<String> params) {
    NewCluster newCluster =
        NewCluster.builder()
            .setNumWorkers(newClusterConfigOptions.getNumWorkers())
            .setNodeTypeId(newClusterConfigOptions.getNodeTypeId())
            .setSparkVersion(newClusterConfigOptions.getSparkVersion())
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
    while (true) {
      JobStatus jobStatus = getJobStatus(job);
      if (jobStatus.isTerminal()) {
        throw new RuntimeException(
            String.format(
                "Failed to submit Databricks job, job state is %s", jobStatus.toString()));
      } else if (jobStatus.equals(JobStatus.RUNNING)) {
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
      throw new RuntimeException(e);
    }
  }
}
