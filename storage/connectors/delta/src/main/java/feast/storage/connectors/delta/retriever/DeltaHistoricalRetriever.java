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
package feast.storage.connectors.delta.retriever;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.value.AutoValue;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import feast.databricks.types.Library;
import feast.databricks.types.NewCluster;
import feast.databricks.types.ObjectMapperFactory;
import feast.databricks.types.RunResultState;
import feast.databricks.types.RunState;
import feast.databricks.types.RunsGetResponse;
import feast.databricks.types.RunsSubmitRequest;
import feast.databricks.types.RunsSubmitResponse;
import feast.databricks.types.SparkJarTask;
import feast.proto.serving.ServingAPIProto;
import feast.proto.serving.ServingAPIProto.HistoricalRetrievalRequest;
import feast.proto.serving.ServingAPIProto.HistoricalRetrievalResult;
import feast.proto.serving.ServingAPIProto.JobStatus;
import feast.storage.api.retriever.HistoricalRetriever;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hc.client5.http.ClientProtocolException;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.slf4j.Logger;

@AutoValue
public abstract class DeltaHistoricalRetriever implements HistoricalRetriever {

  private static final Logger log =
      org.slf4j.LoggerFactory.getLogger(DeltaHistoricalRetriever.class);
  private static final String SPARK_JOB_CLASS =
      "feast.spark.historical.SparkHistoricalRetrieverJob";

  private static ObjectMapper mapper = ObjectMapperFactory.createObjectMapper();

  public static HistoricalRetriever create(Map<String, String> config) {

    return builder()
        .setDatabricksHost(config.get("databricks_host"))
        .setDatabricksToken(config.get("databricks_token"))
        .setDeltaPath(config.get("path"))
        .setJobStagingLocation(config.get("staging_location"))
        .setJarFile(config.get("jar_file"))
        .setNumWorkers(Integer.parseInt(config.get("num_workers")))
        .setSparkVersion(config.get("spark_version"))
        .setSparkConf(config.get("spark_conf"))
        .setInstancePoolId(config.get("instance_pool_id"))
        .setTimeoutSeconds(Integer.parseInt(config.get("timeout_seconds")))
        .build();
  }

  public abstract String databricksHost();

  public abstract String databricksToken();

  public abstract String deltaPath();

  public abstract String jobStagingLocation();

  public abstract String jarFile();

  public abstract int numWorkers();

  public abstract String sparkVersion();

  public abstract String sparkConf();

  public abstract String instancePoolId();

  public abstract int timeoutSeconds();

  public static Builder builder() {
    return new AutoValue_DeltaHistoricalRetriever.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setDatabricksHost(String value);

    public abstract Builder setDatabricksToken(String value);

    public abstract Builder setDeltaPath(String value);

    public abstract Builder setJobStagingLocation(String jobStagingLocation);

    public abstract Builder setJarFile(String value);

    public abstract Builder setNumWorkers(int value);

    public abstract Builder setSparkVersion(String value);

    public abstract Builder setSparkConf(String value);

    public abstract Builder setInstancePoolId(String value);

    public abstract Builder setTimeoutSeconds(int value);

    public abstract DeltaHistoricalRetriever build();
  }

  @Override
  public String getStagingLocation() {
    return jobStagingLocation();
  }

  @Override
  public HistoricalRetrievalResult getHistoricalFeatures(HistoricalRetrievalRequest request) {
    String retrievalId = request.getRetrievalId();

    String exportDestination = String.format("%s/%s", jobStagingLocation(), retrievalId);
    log.info("Starting retrieval {} to {}", retrievalId, exportDestination);
    String requestJson;
    try {
      requestJson = JsonFormat.printer().print(request);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }

    RunsSubmitRequest runRequest =
        getJobRequest(
            "Delta retrieval " + retrievalId,
            Arrays.asList(requestJson, deltaPath(), exportDestination));

    long runId;
    try {
      String body = mapper.writeValueAsString(runRequest);

      log.info("Submitting Databricks run");
      HttpPost httpPost =
          new HttpPost(String.format("%s/api/2.0/jobs/runs/submit", databricksHost()));
      httpPost.setEntity(new StringEntity(body));
      String response = sendDatabricksRequest(httpPost);
      RunsSubmitResponse submitResponse = mapper.readValue(response, RunsSubmitResponse.class);

      runId = submitResponse.getRunId();

    } catch (IOException | InterruptedException e) {
      log.error("Unable to run Databricks job " + retrievalId, e);
      throw new RuntimeException(e);
    }

    log.info("Waiting for Databricks run {} to complete", runId);
    RunState state = waitForJob(runId);

    log.info("Databricks run {} completed with state {}", runId, state);
    if (!Optional.of(RunResultState.SUCCESS).equals(state.getResultState())) {
      return HistoricalRetrievalResult.newBuilder()
          .setId(retrievalId)
          .setError(
              state
                  .getStateMessage()
                  .orElse("Databricks job failed with state " + state.getResultState()))
          .build();
    }
    return HistoricalRetrievalResult.newBuilder()
        .setId(retrievalId)
        .addFileUris(String.format("%s/", exportDestination))
        .setDataFormat(ServingAPIProto.DataFormat.DATA_FORMAT_AVRO)
        .setStatus(JobStatus.JOB_STATUS_DONE)
        .build();
  }

  private RunState waitForJob(long runId) {

    String uri = String.format("%s/api/2.0/jobs/runs/get?run_id=%s", databricksHost(), runId);

    for (int i = 0; ; i++) {
      try {
        if (i > 0) {
          Thread.sleep(10000);
        }
        log.info("Getting job status for Databricks RunId {}", runId);
        HttpGet httpGet = new HttpGet(uri);
        String response = sendDatabricksRequest(httpGet);

        RunsGetResponse runsGetResponse = mapper.readValue(response, RunsGetResponse.class);
        RunState runState = runsGetResponse.getState();
        log.info("Job status for Databricks RunId {} is {}", runId, runState);
        if (runState.getLifeCycleState().isTerminal()) {
          return runState;
        }
      } catch (IOException | InterruptedException e) {
        log.error("Unable to retrieve status of a Databricks run with RunId " + runId, e);
      }
    }
  }

  private String sendDatabricksRequest(ClassicHttpRequest httpPost)
      throws IOException, InterruptedException {

    String authorizationHeader = String.format("%s %s", "Bearer", databricksToken());

    try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
      httpPost.addHeader("Authorization", authorizationHeader);

      try (CloseableHttpResponse response = httpclient.execute(httpPost)) {
        final int status = response.getCode();
        if (status >= HttpStatus.SC_SUCCESS && status < HttpStatus.SC_REDIRECTION) {
          final HttpEntity entity = response.getEntity();
          try {
            return entity != null ? EntityUtils.toString(entity) : null;
          } catch (final ParseException ex) {
            throw new ClientProtocolException(ex);
          }
        } else {
          throw new ClientProtocolException("Unexpected response status: " + status);
        }
      }
    }
  }

  private RunsSubmitRequest getJobRequest(String jobName, List<String> params) {
    Map<String, String> sparkConf =
        Arrays.stream(sparkConf().strip().split("\n"))
            .map(s -> s.strip().split("\\s+", 2))
            .collect(Collectors.toMap(s -> s[0], s -> s[1]));

    NewCluster.Builder newCluster =
        NewCluster.builder()
            .setNumWorkers(numWorkers())
            .setInstancePoolId(instancePoolId())
            .setSparkVersion(sparkVersion())
            .setSparkConf(sparkConf);

    Library library = Library.builder().setJar(jarFile()).build();
    List<Library> libraries = Collections.singletonList(library);

    SparkJarTask sparkJarTask =
        SparkJarTask.builder().setMainClassName(SPARK_JOB_CLASS).setParameters(params).build();

    RunsSubmitRequest runRequest =
        RunsSubmitRequest.builder()
            .setLibraries(libraries)
            .setTimeoutSeconds(timeoutSeconds())
            .setName(jobName)
            .setNewCluster(newCluster.build())
            .setSparkJarTask(sparkJarTask)
            .build();

    return runRequest;
  }
}
