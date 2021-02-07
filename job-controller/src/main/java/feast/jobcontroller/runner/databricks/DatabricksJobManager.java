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
package feast.jobcontroller.runner.databricks;

import static feast.databricks.types.ObjectMapperFactory.createObjectMapper;
import static feast.jobcontroller.runner.databricks.DatabricksRunStateMapper.mapJobState;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import feast.databricks.types.Library;
import feast.databricks.types.NewCluster;
import feast.databricks.types.RunState;
import feast.databricks.types.RunsCancelRequest;
import feast.databricks.types.RunsGetResponse;
import feast.databricks.types.RunsListResponse;
import feast.databricks.types.RunsSubmitRequest;
import feast.databricks.types.RunsSubmitResponse;
import feast.databricks.types.SparkJarTask;
import feast.jobcontroller.config.FeastProperties.MetricsProperties;
import feast.jobcontroller.exception.JobExecutionException;
import feast.jobcontroller.exception.JobMonitoringException;
import feast.jobcontroller.model.Job;
import feast.jobcontroller.model.JobStatus;
import feast.jobcontroller.runner.JobManager;
import feast.jobcontroller.runner.Runner;
import feast.proto.core.IngestionJobProto.SpecsStreamingUpdateConfig;
import feast.proto.core.RunnerProto.DatabricksRunnerConfigOptions;
import feast.proto.core.SourceProto.Source;
import feast.proto.core.StoreProto;
import feast.proto.core.StoreProto.Store;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.Charsets;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class DatabricksJobManager implements JobManager {

  private static final String SPARK_INGESTION_CLASS = "feast.spark.ingestion.SparkIngestion";

  private static final Runner RUNNER_TYPE = Runner.DATABRICKS;

  private final String databricksHost;
  private final byte[] databricksToken;
  private final String checkpointLocation;
  private final String jarFile;
  private final DatabricksRunnerConfigOptions.DatabricksNewClusterOptions newClusterConfigOptions;
  private final String deadLetterPath;
  private final SpecsStreamingUpdateConfig specsStreamingUpdateConfig;
  private final HttpClient httpClient;
  private static final ObjectMapper mapper = createObjectMapper();

  private final int timeoutSeconds;
  private final MetricsProperties metrics;

  public DatabricksJobManager(
      DatabricksRunnerConfigOptions runnerConfigOptions,
      MetricsProperties metrics,
      SpecsStreamingUpdateConfig specsStreamingUpdateConfig,
      HttpClient httpClient) {

    this.databricksHost = runnerConfigOptions.getHost();
    this.databricksToken = runnerConfigOptions.getToken().getBytes(StandardCharsets.UTF_8);
    this.checkpointLocation = runnerConfigOptions.getCheckpointLocation();
    this.metrics = metrics;
    this.httpClient = httpClient;
    this.newClusterConfigOptions = runnerConfigOptions.getNewCluster();
    this.deadLetterPath = runnerConfigOptions.getDeadLetterPath();
    this.jarFile = runnerConfigOptions.getJarFile();
    this.timeoutSeconds = runnerConfigOptions.getTimeoutSeconds();
    this.specsStreamingUpdateConfig = specsStreamingUpdateConfig;
  }

  @Override
  public Runner getRunnerType() {
    return RUNNER_TYPE;
  }

  @Override
  public Job startJob(Job job) {
    String jobId = job.getId();

    long databricksRunId = createDatabricksRun(job);

    log.info("Setting job status for job {} (Databricks RunId {})", jobId, databricksRunId);
    job.setExtId(Long.toString(databricksRunId));

    log.info("Waiting for job {} to start (Databricks RunId {})", jobId, databricksRunId);
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
  public Job abortJob(Job job) {
    String runId = job.getExtId();
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

    } catch (IOException | InterruptedException e) {
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
    job.setStatus(JobStatus.ABORTING);
    return job;
  }

  private URI getAbortUri() {
    return URI.create(String.format("%s/api/2.0/jobs/runs/cancel", databricksHost));
  }

  @Override
  public Job restartJob(Job job) {
    abortJob(job);
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

      JobStatus status = mapJobState(runState);

      log.info(
          "Databricks job state for job {} (Databricks RunId {}) is {} (mapped to {})",
          job.getId(),
          job.getExtId(),
          runState,
          status);
      return status;

    } catch (IOException | InterruptedException ex) {
      log.error("Unable to retrieve status of a databricks run with id " + job.getExtId(), ex);
      return JobStatus.UNKNOWN;
    }
  }

  @Override
  public List<Job> listRunningJobs() {
    log.info("Listing currently active databricks runs");

    try {
      HttpRequest.Builder request =
          HttpRequest.newBuilder()
              .uri(
                  URI.create(
                      String.format("%s/api/2.0/jobs/runs/list?active_only=true", databricksHost)))
              .GET();

      HttpResponse<String> response = sendDatabricksRequest(request);
      RunsListResponse runsListResponse = mapper.readValue(response.body(), RunsListResponse.class);

      if (runsListResponse.getRuns().isEmpty()) {
        return Collections.emptyList();
      }
      return runsListResponse.getRuns().get().stream()
          .map(
              run -> {
                Map<String, StoreProto.Store> stores = new HashMap<>();
                Source source = null;
                Optional<SparkJarTask> sparkJarTask = run.getTask().getSparkJarTask();
                if (sparkJarTask.isPresent()) {
                  Optional<List<String>> parameters = sparkJarTask.get().getParameters();
                  if (parameters.isPresent()) {
                    try {
                      // Stores are \n separated json format at index 5
                      String[] storeJsons = parameters.get().get(5).split("\n");
                      for (String storeJson : storeJsons) {
                        Store.Builder storeBuilder = Store.newBuilder();
                        JsonFormat.parser().merge(storeJson, storeBuilder);
                        Store store = storeBuilder.build();
                        stores.put(store.getName(), store);
                      }

                      // The source is json format at index 6
                      String sourceJson = parameters.get().get(6);
                      Source.Builder sourceBuilder = Source.newBuilder();
                      JsonFormat.parser().merge(sourceJson, sourceBuilder);
                      source = sourceBuilder.build();
                    } catch (IndexOutOfBoundsException | InvalidProtocolBufferException e) {
                      throw new JobMonitoringException(
                          String.format(
                              "Unable to retrieve list of active databricks runs: %s\ncause: %s",
                              e, e.getCause()),
                          e);
                    }
                  }
                }

                Job job =
                    Job.builder()
                        .setId(run.getRunName())
                        .setStores(stores)
                        .setSource(source)
                        .setFeatureSetDeliveryStatuses(Collections.emptyMap()) // No way to extract
                        .setLabels(Collections.emptyMap()) // Not storing any
                        .build();
                if (run.getRunId() != null) {
                  job.setExtId(String.valueOf(run.getRunId()));
                }
                job.setStatus(mapJobState(run.getState()));
                if (run.getStartTime() != null) {
                  job.setCreated(new Date(run.getStartTime()));
                }

                return job;
              })
          .collect(Collectors.toList());

    } catch (IOException | InterruptedException e) {
      log.error("Unable to retrieve list of active databricks runs", e);
      throw new JobMonitoringException(
          String.format(
              "Unable to retrieve list of active databricks runs: %s\ncause: %s", e, e.getCause()),
          e);
    }
  }

  private URI getJobUri(Job job) {
    return URI.create(
        String.format("%s/api/2.0/jobs/runs/get?run_id=%s", databricksHost, job.getExtId()));
  }

  private HttpResponse<String> sendDatabricksRequest(HttpRequest.Builder builder)
      throws IOException, InterruptedException {

    String authorizationHeader =
        String.format("%s %s", "Bearer", new String(databricksToken, StandardCharsets.UTF_8));

    HttpRequest request = builder.header("Authorization", authorizationHeader).build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      throw new IOException(
          String.format(
              "Databricks returned with unexpected code: %s -- %s",
              response.statusCode(), response.body()));
    }
    return response;
  }

  private long createDatabricksRun(Job job) {
    log.info("Starting job id {}", job.getId());

    String jobName = String.format("Feast ingestion job %s", job.getId());
    Collection<StoreProto.Store> stores = job.getStores().values();

    String storesJson = toJsonLines(stores);
    String specsStreamingUpdateConfigJson = toJsonLine(specsStreamingUpdateConfig);
    String sourceJson = toJsonLine(job.getSource());

    String statsdHost;
    String statsdPort;
    if (metrics.isEnabled()) {
      statsdHost = metrics.getHost();
      statsdPort = String.valueOf(metrics.getPort());
    } else {
      statsdHost = "";
      statsdPort = "";
    }

    List<String> params =
        Arrays.asList(
            job.getId(),
            specsStreamingUpdateConfigJson,
            checkpointLocation,
            "default",
            deadLetterPath,
            storesJson,
            sourceJson,
            metrics.getType(),
            statsdHost,
            statsdPort);

    RunsSubmitRequest runRequest = getJobRequest(jobName, params);

    try {
      String body = mapper.writeValueAsString(runRequest);

      HttpRequest.Builder request =
          HttpRequest.newBuilder()
              .uri(URI.create(String.format("%s/api/2.0/jobs/runs/submit", databricksHost)))
              .POST(HttpRequest.BodyPublishers.ofString(body));

      HttpResponse<String> response = sendDatabricksRequest(request);
      RunsSubmitResponse submitResponse =
          mapper.readValue(response.body(), RunsSubmitResponse.class);

      return submitResponse.getRunId();

    } catch (IOException | InterruptedException e) {
      log.error("Unable to run databricks job" + jobName, e);
      throw new JobExecutionException(
          String.format(
              "Unable to run databricks job : %s \nmessage: %s\ncause: %s",
              jobName, e, e.getCause()),
          e);
    }
  }

  private RunsSubmitRequest getJobRequest(String jobName, List<String> params) {
    Map<String, String> sparkConf =
        Arrays.stream(newClusterConfigOptions.getSparkConf().strip().split("\n"))
            .map(s -> s.strip().split("\\s+", 2))
            .collect(Collectors.toMap(s -> s[0], s -> s[1]));

    NewCluster.Builder newCluster =
        NewCluster.builder()
            .setNumWorkers(newClusterConfigOptions.getNumWorkers())
            .setSparkVersion(newClusterConfigOptions.getSparkVersion())
            .setSparkConf(sparkConf);

    ifPresent(newClusterConfigOptions.getNodeTypeId(), newCluster::setNodeTypeId);
    ifPresent(newClusterConfigOptions.getInstancePoolId(), newCluster::setInstancePoolId);

    Library library = Library.builder().setJar(jarFile).build();
    List<Library> libraries = Collections.singletonList(library);

    SparkJarTask sparkJarTask =
        SparkJarTask.builder()
            .setMainClassName(SPARK_INGESTION_CLASS)
            .setParameters(params)
            .build();

    String token = toDigest(jobName);

    RunsSubmitRequest runRequest =
        RunsSubmitRequest.builder()
            .setLibraries(libraries)
            .setTimeoutSeconds(timeoutSeconds)
            .setRunName(jobName)
            .setNewCluster(newCluster.build())
            .setSparkJarTask(sparkJarTask)
            .setIdempotencyToken(token)
            .build();

    return runRequest;
  }

  /**
   * Create a 64-character long digest from a string. This can be used as the Databricks
   * idempotency_token, which is <a
   * href="https://docs.databricks.com/dev-tools/api/latest/jobs.html#request-structure">limited to
   * 64 characters</a>.
   *
   * @param jobName String to digest
   * @return a digest exactly 64 characters long.
   */
  static String toDigest(String jobName) {
    MessageDigest md;
    try {
      md = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }

    md.update(jobName.getBytes(Charsets.UTF_8));
    String token = Hex.encodeHexString(md.digest());
    return token;
  }

  private static void ifPresent(String nodeTypeId, Consumer<? super String> action) {
    asOptional(nodeTypeId).ifPresent(action);
  }

  private static Optional<String> asOptional(String field) {
    return Optional.ofNullable(StringUtils.trimToNull(field));
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
