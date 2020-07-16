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
import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import feast.core.config.FeastProperties.MetricsProperties;
import feast.core.exception.JobExecutionException;
import feast.core.job.Runner;
import feast.core.model.*;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.IngestionJobProto;
import feast.proto.core.IngestionJobProto.SpecsStreamingUpdateConfig;
import feast.proto.core.RunnerProto.DatabricksRunnerConfigOptions;
import feast.proto.core.SourceProto;
import feast.proto.core.SourceProto.KafkaSourceConfig;
import feast.proto.core.StoreProto;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;

public class DatabricksJobManagerTest {

  private static final int SC_OK = 200;

  private static final int SC_BAD_REQUEST = 400;

  @Rule public final ExpectedException expectedException = ExpectedException.none();

  @Mock private HttpClient httpClient;

  @Mock private HttpResponse<Object> httpResponse;

  private DatabricksJobManager dbJobManager;
  private Job job;

  @Before
  public void setUp() {
    initMocks(this);

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

    MetricsProperties metricsProperties = new MetricsProperties();
    metricsProperties.setEnabled(false);
    DatabricksRunnerConfigOptions.DatabricksNewClusterOptions newClusterConfigOptions =
        DatabricksRunnerConfigOptions.DatabricksNewClusterOptions.newBuilder()
            .setSparkConf("spark.driver.memory 1G")
            .build();

    DatabricksRunnerConfigOptions.Builder databricksRunnerConfigOptions =
        DatabricksRunnerConfigOptions.newBuilder()
            .setNewCluster(newClusterConfigOptions)
            .setToken("TOKEN")
            .setHost("https://databricks");

    FeatureSetJobStatus featureSetJobStatus = new FeatureSetJobStatus();
    featureSetJobStatus.setFeatureSet(FeatureSet.fromProto(featureSet));

    SpecsStreamingUpdateConfig specsStreamingUpdateConfig =
        IngestionJobProto.SpecsStreamingUpdateConfig.newBuilder()
            .setSource(
                KafkaSourceConfig.newBuilder()
                    .setTopic("specs_topic")
                    .setBootstrapServers("servers:9092")
                    .build())
            .build();

    this.job =
        Job.builder()
            .setId("1")
            .setExtId("")
            .setRunner(Runner.DATABRICKS)
            .setSource(Source.fromProto(source))
            .setFeatureSetJobStatuses(Sets.newHashSet(featureSetJobStatus))
            .setStatus(JobStatus.PENDING)
            .build();
    job.setStores(ImmutableSet.of(Store.fromProto(store)));
    dbJobManager =
        new DatabricksJobManager(
            databricksRunnerConfigOptions.build(), specsStreamingUpdateConfig, httpClient);
  }

  @Test
  public void testGetCalltoDatabricksWithOnlyLifeCycle() throws IOException, InterruptedException {
    String responseBody =
        "{ \"state\": {\"life_cycle_state\" : \"INTERNAL_ERROR\", \"state_message\": \"a state message\"} } ";
    when(httpResponse.body()).thenReturn(responseBody);
    when(httpResponse.statusCode()).thenReturn(SC_OK);
    when(httpClient.send(any(), any())).thenReturn(httpResponse);

    JobStatus jobStatus = dbJobManager.getJobStatus(job);
    assertThat(jobStatus, equalTo(JobStatus.ERROR));
  }

  @Test
  public void testGetCalltoDatabricksWithLifeCycleAndRunState()
      throws IOException, InterruptedException {
    String responseBody =
        "{ \"state\": {\"life_cycle_state\" : \"TERMINATED\", \"result_state\": \"SUCCESS\", \"state_message\": \"a state message\" } } ";

    when(httpResponse.body()).thenReturn(responseBody);
    when(httpResponse.statusCode()).thenReturn(SC_OK);
    when(httpClient.send(any(), any())).thenReturn(httpResponse);

    JobStatus jobStatus = dbJobManager.getJobStatus(job);
    assertThat(jobStatus, equalTo(JobStatus.COMPLETED));
  }

  @Test
  public void testStartJob_OK() throws IOException, InterruptedException {
    String runsSubmitResponseBody = "{ \"run_id\" : \"10\"} ";
    String jobStatusResponseBody =
        "{ \"state\": {\"life_cycle_state\" : \"RUNNING\", \"state_message\": \"a state message\"} } ";

    when(httpResponse.body()).thenReturn(runsSubmitResponseBody).thenReturn(jobStatusResponseBody);
    when(httpResponse.statusCode()).thenReturn(SC_OK);
    when(httpClient.send(any(), any())).thenReturn(httpResponse);

    Job actual = dbJobManager.startJob(job);

    assertThat(actual.getExtId(), equalTo("10"));
    assertThat(actual.getId(), equalTo(job.getId()));
    assertThat(actual.getStatus(), equalTo(JobStatus.RUNNING));
  }

  @Test(expected = JobExecutionException.class)
  public void testStartJob_BadRequest() throws IOException, InterruptedException {
    when(httpResponse.statusCode()).thenReturn(SC_BAD_REQUEST);
    when(httpClient.send(any(), any())).thenReturn(httpResponse);

    dbJobManager.startJob(job);

    verify(httpClient, Mockito.times(1)).send(any(), any());
  }

  @Test
  public void testAbortJob_OKRequest() throws IOException, InterruptedException {
    job.setExtId("1");

    when(httpResponse.statusCode()).thenReturn(SC_OK);
    when(httpClient.send(any(), any())).thenReturn(httpResponse);

    dbJobManager.abortJob(job);
    Job actual = dbJobManager.abortJob(job);

    verify(httpClient, Mockito.times(1)).send(any(), any());
    assertThat(actual.getStatus(), equalTo(JobStatus.ABORTING));
  }

  @Test(expected = JobExecutionException.class)
  public void testAbortJob_BadRequest() throws IOException, InterruptedException {
    when(httpResponse.statusCode()).thenReturn(SC_BAD_REQUEST);
    when(httpClient.send(any(), any())).thenReturn(httpResponse);

    dbJobManager.startJob(job);

    verify(httpClient, Mockito.times(1)).send(any(), any());
  }

  @Test
  public void toDigest() throws Exception {
    String digest1 = DatabricksJobManager.toDigest("somestring");
    assertThat(digest1.length(), equalTo(64));
    String digest2 = DatabricksJobManager.toDigest("");
    assertThat(digest2.length(), equalTo(64));
    assertThat(digest1, not(digest2));
  }
}
