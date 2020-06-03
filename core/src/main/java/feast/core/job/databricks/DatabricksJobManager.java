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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.config.FeastProperties.MetricsProperties;
import feast.core.exception.JobExecutionException;
import feast.core.job.JobManager;
import feast.core.job.Runner;
import feast.core.model.*;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import feast.databricks.types.RunsSubmitRequest;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.SourceProto;
import feast.proto.core.StoreProto;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpException;
import org.apache.http.HttpStatus;

@Slf4j
public class DatabricksJobManager implements JobManager {
  private final Runner RUNNER_TYPE = Runner.DATABRICKS;

  private final String databricksHost;
  private final String databricksToken;
  private final Map<String, String> defaultOptions;
  private final MetricsProperties metricsProperties;
  private final HttpClient httpClient;
  private final ObjectMapper mapper = new ObjectMapper();

  public DatabricksJobManager(
      Map<String, String> runnerConfigOptions,
      MetricsProperties metricsProperties,
      String token,
      HttpClient httpClient) {

    this.databricksHost = runnerConfigOptions.get("databricksHost");
    this.defaultOptions = runnerConfigOptions;
    this.metricsProperties = metricsProperties;
    this.httpClient = httpClient;
    this.databricksToken = token;

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

      return runDatabricksJob(
          job.getId(), featureSetProtos, job.getSource().toProto(), job.getStore().toProto());
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
                        "%s/api/2.0/jobs/runs/get?run_id=%s", this.databricksHost, job.getExtId())))
            .header("Authorization", String.format("%s %s", "Bearer", this.databricksToken))
            .build();
    try {
      HttpResponse<String> response =
          this.httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() == HttpStatus.SC_OK) {
        JsonNode parent = mapper.readTree(response.body());
        Optional<JsonNode> resultState =
            Optional.ofNullable(parent.path("state").get("result_state"));
        String lifeCycleState = parent.path("state").get("life_cycle_state").asText().toUpperCase();

        if (resultState.isPresent()) {
          return DatabricksJobStateMapper.map(
              String.format("%s_%s", lifeCycleState, resultState.get().asText().toUpperCase()));
        }

        return DatabricksJobStateMapper.map(lifeCycleState);
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

  @SneakyThrows
  private Job runDatabricksJob(
      String jobId,
      List<FeatureSetProto.FeatureSet> featureSetProtos,
      SourceProto.Source source,
      StoreProto.Store sink) {

    List<FeatureSet> featureSets =
        featureSetProtos.stream().map(FeatureSet::fromProto).collect(Collectors.toList());


    RunsSubmitRequest runsSubmitRequest = new RunsSubmitRequest();
    runsSubmitRequest.setRun_name(jobId);

    // TODO: build RunsSubmitRequest from the other types

    String body = mapper.writeValueAsString(runsSubmitRequest);

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(String.format("%s/api/2.0/jobs/run-submit", this.databricksHost)))
            .header("Authorization", String.format("%s %s", "Bearer", this.databricksToken))
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();

    try {
      HttpResponse<String> response =
          this.httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() == HttpStatus.SC_OK) {
        JsonNode parent = new ObjectMapper().readTree(response.body());
        String runId = parent.path("run_id").asText(); // TODO: use runsubmitresponse class
        // wait for status running
        return new Job(
            jobId,
            runId,
            getRunnerType(),
            Source.fromProto(source),
            Store.fromProto(sink),
            featureSets,
            JobStatus.RUNNING);
      } else {
        throw new RuntimeException(
            String.format(
                "Failed running of job %s: %s",
                jobId, response.body())); // TODO: change to handle failure
      }
    } catch (IOException | InterruptedException e) {
      log.error("Unable to run databricks job with id : {}\ncause: {}", jobId, e.getMessage());
      throw new JobExecutionException(
          String.format("Unable to run databricks job with id : %s\ncause: %s", jobId, e), e);
    }
  }

  // TODO: look into waitForRun to check job has started successfully.

}
