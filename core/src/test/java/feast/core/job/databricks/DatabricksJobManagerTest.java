/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.collect.Lists;
import feast.core.config.FeastProperties.MetricsProperties;
import feast.core.job.Runner;
import feast.core.model.*;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.RunnerProto.DatabricksRunnerConfigOptions;
import feast.proto.core.SourceProto;
import feast.proto.core.StoreProto;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;

public class DatabricksJobManagerTest {

  @Rule public final ExpectedException expectedException = ExpectedException.none();

  @Mock private Job job;

  @Mock private HttpClient httpClient;

  private Map<String, String> runnerConfigOptions;
  private DatabricksJobManager dbJobManager;
  private String token;

  @Before
  public void setUp() {
    initMocks(this);
    MetricsProperties metricsProperties = new MetricsProperties();
    metricsProperties.setEnabled(false);
  }

  @Test
  public void testGetCalltoDatabricksWithOnlyLifeCycle() throws IOException, InterruptedException {
    Mockito.when(job.getExtId()).thenReturn("1");
    HttpResponse httpResponse = mock(HttpResponse.class);
    String responseBody =
        "{ \"state\": {\"life_cycle_state\" : \"INTERNAL_ERROR\", \"state_message\": \"a state message\"} } ";
    when(httpResponse.body()).thenReturn(responseBody);
    when(httpResponse.statusCode()).thenReturn(200);

    Mockito.when(httpClient.send(any(), any())).thenReturn(httpResponse);
    MetricsProperties metricsProperties = new MetricsProperties();
    metricsProperties.setEnabled(false);
    DatabricksRunnerConfigOptions.Builder databricksRunnerConfigOptions =
        DatabricksRunnerConfigOptions.newBuilder();

    databricksRunnerConfigOptions.setHost("https://databricks");

    dbJobManager =
        new DatabricksJobManager(
            databricksRunnerConfigOptions.build(), metricsProperties, httpClient);
    dbJobManager = spy(dbJobManager);

    JobStatus jobStatus = dbJobManager.getJobStatus(job);
    assertThat(jobStatus, equalTo(JobStatus.ERROR));
  }

  @Test
  public void testGetCalltoDatabricksWithLifeCycleAndRunState()
      throws IOException, InterruptedException {
    Mockito.when(job.getExtId()).thenReturn("1");
    HttpResponse httpResponse = mock(HttpResponse.class);
    String responseBody =
        "{ \"state\": {\"life_cycle_state\" : \"TERMINATED\", \"result_state\": \"SUCCESS\", \"state_message\": \"a state message\" } } ";
    when(httpResponse.body()).thenReturn(responseBody);
    when(httpResponse.statusCode()).thenReturn(200);

    Mockito.when(httpClient.send(any(), any())).thenReturn(httpResponse);
    MetricsProperties metricsProperties = new MetricsProperties();
    metricsProperties.setEnabled(false);

    DatabricksRunnerConfigOptions.Builder databricksRunnerConfigOptions =
        DatabricksRunnerConfigOptions.newBuilder();

    databricksRunnerConfigOptions.setHost("https://databricks");

    dbJobManager =
        new DatabricksJobManager(
            databricksRunnerConfigOptions.build(), metricsProperties, httpClient);
    dbJobManager = spy(dbJobManager);

    JobStatus jobStatus = dbJobManager.getJobStatus(job);
    assertThat(jobStatus, equalTo(JobStatus.COMPLETED));
  }

  @Test
  public void testStartJob() throws IOException, InterruptedException {
    StoreProto.Store store =
        StoreProto.Store.newBuilder()
            .setName("SERVING")
            .setType(StoreProto.Store.StoreType.REDIS)
            .setRedisConfig(
                StoreProto.Store.RedisConfig.newBuilder()
                    .setHost("localhost")
                    .setPort(6379)
                    .build())
            .addSubscriptions(
                StoreProto.Store.Subscription.newBuilder().setProject("*").setName("*").build())
            .build();

    SourceProto.Source source =
        SourceProto.Source.newBuilder()
            .setType(SourceProto.SourceType.KAFKA)
            .setKafkaSourceConfig(
                SourceProto.KafkaSourceConfig.newBuilder()
                    .setTopic("topic")
                    .setBootstrapServers("servers:9092")
                    .build())
            .build();

    FeatureSetProto.FeatureSet featureSet =
        FeatureSetProto.FeatureSet.newBuilder()
            .setSpec(
                FeatureSetProto.FeatureSetSpec.newBuilder()
                    .setName("featureSet")
                    .setSource(source)
                    .build())
            .build();

    HttpResponse httpResponse = mock(HttpResponse.class);
    String createResponseBody = "{ \"job_id\" : \"5\" } ";
    String runNowResponseBody = "{ \"run_id\" : \"10\" } ";
    String jobStatusResponseBody =
        "{ \"state\": {\"life_cycle_state\" : \"RUNNING\", \"state_message\": \"a state message\"} } ";

    when(httpResponse.body())
        .thenReturn(createResponseBody)
        .thenReturn(runNowResponseBody)
        .thenReturn(jobStatusResponseBody);
    when(httpResponse.statusCode()).thenReturn(200);

    Mockito.when(httpClient.send(any(), any())).thenReturn(httpResponse);
    HttpClient mockHttpClient = Mockito.mock(HttpClient.class);

    MetricsProperties metricsProperties = new MetricsProperties();
    metricsProperties.setEnabled(false);
    DatabricksRunnerConfigOptions.Builder databricksRunnerConfigOptions =
        DatabricksRunnerConfigOptions.newBuilder();

    databricksRunnerConfigOptions.setHost("https://databricks");

    dbJobManager =
        new DatabricksJobManager(
            databricksRunnerConfigOptions.build(), metricsProperties, httpClient);
    dbJobManager = spy(dbJobManager);

    doReturn(httpResponse).when(mockHttpClient).send(any(), any());

    Job job =
        new Job(
            "3",
            "",
            Runner.DATABRICKS,
            Source.fromProto(source),
            Store.fromProto(store),
            Lists.newArrayList(FeatureSet.fromProto(featureSet)),
            JobStatus.PENDING);
    Job actual = dbJobManager.startJob(job);

    assertThat(actual.getExtId(), equalTo("10"));
    assertThat(actual.getId(), equalTo(job.getId()));
  }

  @Test
  public void testAbortJob() throws IOException, InterruptedException {
    Mockito.when(job.getExtId()).thenReturn("run-id"); // TODO: replace with a valid tun id to run

    MetricsProperties metricsProperties = new MetricsProperties();
    metricsProperties.setEnabled(false);

    DatabricksRunnerConfigOptions.Builder databricksRunnerConfigOptions =
        DatabricksRunnerConfigOptions.newBuilder();

    databricksRunnerConfigOptions.setHost("https://adb-8918595472279780.0.azuredatabricks.net");

    databricksRunnerConfigOptions.setToken(
        "TOKEN"); // TODO: replace with a valid databricks token to run

    dbJobManager =
        new DatabricksJobManager(
            databricksRunnerConfigOptions.build(), metricsProperties, httpClient);
    dbJobManager = spy(dbJobManager);

    dbJobManager.abortJob(job.getExtId());
  }
}
