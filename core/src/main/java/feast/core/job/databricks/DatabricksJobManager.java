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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.config.FeastProperties.MetricsProperties;
import feast.core.exception.JobExecutionException;
import feast.core.job.JobManager;
import feast.core.job.Runner;
import feast.core.model.*;
import feast.databricks.types.*;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.RunnerProto.DatabricksRunnerConfigOptions;
import feast.proto.core.SourceProto;
import feast.proto.core.StoreProto;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpException;
import org.apache.http.HttpStatus;

@Slf4j
public class DatabricksJobManager implements JobManager {
  private final Runner RUNNER_TYPE = Runner.DATABRICKS;

  private final String databricksHost;
  private final byte[] databricksToken;
  private final String jarFile;
  private final DatabricksRunnerConfigOptions.DatabricksNewClusterOptions newClusterConfigOptions;
  private final DatabricksRunnerConfigOptions.DatabricksSparkJarTaskOptions sparkJarTaskOptions;
  private final MetricsProperties metricsProperties;
  private final HttpClient httpClient;
  private final ObjectMapper mapper = new ObjectMapper();
  private final int maxRetries = -1;

  public DatabricksJobManager(
      DatabricksRunnerConfigOptions runnerConfigOptions,
      MetricsProperties metricsProperties,
      HttpClient httpClient) {

    this.databricksHost = runnerConfigOptions.getHost();
    this.databricksToken = runnerConfigOptions.getToken().getBytes();
    this.metricsProperties = metricsProperties;
    this.httpClient = httpClient;
    this.sparkJarTaskOptions = runnerConfigOptions.getSparkJarTask();
    this.newClusterConfigOptions = runnerConfigOptions.getNewCluster();
    this.jarFile = runnerConfigOptions.getJarFile();
  }

  @Override
  public Runner getRunnerType() {
    return RUNNER_TYPE;
  }

  @Override
  public Job startJob(Job job) {
    try {
      List<FeatureSetProto.FeatureSet> featureSetProtos = new ArrayList<>();
      for (FeatureSet featureSet : job.getFeatureSets()) {
        featureSetProtos.add(featureSet.toProto());
      }

      long databricksJobId = createDatabricksJob(job.getId());
      return runDatabricksJob(
          job.getId(),
          databricksJobId,
          featureSetProtos,
          job.getSource().toProto(),
          job.getStore().toProto());

    } catch (InvalidProtocolBufferException e) {
      log.error(e.getMessage());
      throw new IllegalArgumentException(
          String.format(
              "DatabricksJobManager failed to START job with id '%s' because the job"
                  + "has an invalid spec. Please check the FeatureSet, Source and Store specs. Actual error message: %s",
              job.getId(), e.getMessage()));
    }
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
  public void abortJob(String jobId) {}

  @Override
  public Job restartJob(Job job) {
    abortJob(job.getExtId());
    return startJob(job);
  }

  @Override
  public JobStatus getJobStatus(Job job) {
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(
                URI.create(
                    String.format(
                        "%s/api/2.0/jobs/runs/get?run_id=%s", databricksHost, job.getExtId())))
            .header("Authorization", getAuthorizationHeader())
            .build();
    try {
      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() == HttpStatus.SC_OK) {
        RunsGetResponse runsGetResponse = mapper.readValue(response.body(), RunsGetResponse.class);
        RunState runState = runsGetResponse.getState();
        String lifeCycleState = runState.getLifeCycleState().toString();

        return runState.getResultState()
                .map(runResultState -> DatabricksJobStateMapper.map(String.format("%s_%s", lifeCycleState, runResultState.toString())))
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
    return String.format("%s %s", "Bearer", Arrays.toString(databricksToken));
  }

  private long createDatabricksJob(String jobId) {

    JobsCreateRequest createRequest = getJobRequest(jobId);

    try {
      String body = mapper.writeValueAsString(createRequest);

      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(String.format("%s/api/2.0/jobs/create", databricksHost)))
              .header("Authorization", getAuthorizationHeader())
              .POST(HttpRequest.BodyPublishers.ofString(body))
              .build();

      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() == HttpStatus.SC_OK) {
        JobsCreateResponse createResponse =
            mapper.readValue(response.body(), JobsCreateResponse.class);

        return createResponse.getJobId();
      } else {
        throw new HttpException(
            String.format("Databricks returned with unexpected code: %s", response.statusCode()));
      }
    } catch (IOException | InterruptedException | HttpException e) {
      log.error("Unable to run databricks job with id : {}\ncause: {}", jobId, e.getMessage());
      throw new JobExecutionException(
          String.format("Unable to run databricks job with id : %s\ncause: %s", jobId, e), e);
    }
  }

  private Job runDatabricksJob(
      String jobId,
      long databricksJobId,
      List<FeatureSetProto.FeatureSet> featureSetProtos,
      SourceProto.Source source,
      StoreProto.Store sink) {
    RunNowRequest runNowRequest = getRunNowRequest(databricksJobId, source);

    List<FeatureSet> featureSets = new ArrayList<>();
    for (FeatureSetProto.FeatureSet featureSetProto : featureSetProtos) {
      featureSets.add(FeatureSet.fromProto(featureSetProto));
    }

    try {
      String body = mapper.writeValueAsString(runNowRequest);

      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(String.format("%s/api/2.0/jobs/run-now", databricksHost)))
              .header("Authorization", getAuthorizationHeader())
              .POST(HttpRequest.BodyPublishers.ofString(body))
              .build();

      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() == HttpStatus.SC_OK) {
        RunNowResponse runNowResponse = mapper.readValue(response.body(), RunNowResponse.class);
        Job job =
            new Job(
                jobId,
                String.valueOf(runNowResponse.getRunId()),
                getRunnerType(),
                Source.fromProto(source),
                Store.fromProto(sink),
                featureSets,
                JobStatus.RUNNING);

        waitForJobToRun(job);

        return job;
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

  private RunNowRequest getRunNowRequest(long databricksJobId, SourceProto.Source source) {

    SourceProto.KafkaSourceConfig kafkaSourceConfig = source.getKafkaSourceConfig();

    List<String> jarParams =
        Arrays.asList(kafkaSourceConfig.getBootstrapServers(), kafkaSourceConfig.getTopic());

    RunNowRequest runNowRequest = RunNowRequest.builder().setJobId(databricksJobId).setJarParams(jarParams).build();

    return runNowRequest;
  }

  private JobsCreateRequest getJobRequest(String jobId) {
    NewCluster newCluster =
        NewCluster.builder()
            .setNumWorkers(newClusterConfigOptions.getNumWorkers())
            .setNodeTypeId(newClusterConfigOptions.getNodeTypeId())
            .setSparkVersion(newClusterConfigOptions.getSparkVersion())
            .build();

    List<Library> libraries = new ArrayList<>();
    Library library = Library.builder().setJar(jarFile).build();
    libraries.add(library);

    SparkJarTask sparkJarTask =
        SparkJarTask.builder()
            .setMainClassName(sparkJarTaskOptions.getMainClassName())
            .build();

    JobsCreateRequest createRequest =
        JobsCreateRequest.builder()
            .setLibraries(libraries)
            .setMaxRetries(maxRetries)
            .setName(jobId)
            .setNewCluster(newCluster)
            .setSparkJarTask(sparkJarTask)
            .build();

    return createRequest;
  }

  private void waitForJobToRun(Job job) throws InterruptedException {
    while (true) {
      JobStatus jobStatus = getJobStatus(job);
      if (jobStatus.isTerminal()) {
        throw new RuntimeException();
      } else if (jobStatus.equals(JobStatus.RUNNING)) {
        break;
      }
      Thread.sleep(2000);
    }
  }
}
