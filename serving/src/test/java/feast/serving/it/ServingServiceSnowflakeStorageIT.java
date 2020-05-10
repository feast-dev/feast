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
package feast.serving.it;

import static org.awaitility.Awaitility.waitAtMost;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.beans.HasPropertyWithValue.hasProperty;
import static org.junit.jupiter.api.Assertions.*;

import com.google.protobuf.ProtocolStringList;
import feast.proto.serving.ServingAPIProto.*;
import feast.proto.serving.ServingServiceGrpc.ServingServiceBlockingStub;
import feast.proto.types.FeatureRowProto.FeatureRow;
import io.grpc.StatusRuntimeException;
import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.runners.model.InitializationError;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Ignore("This manual test needs a testing Snowflake account!")
@ActiveProfiles("it")
@SpringBootTest(properties = {"feast.active_store=historical_snowflake"})
@Testcontainers
public class ServingServiceSnowflakeStorageIT {
  /**
   * Manual test needs a testing Snowflake, and AWS S3 bucket accounts Run this manual e2e test with
   * snowflake and s3 bucket configs 1. Update application-it.properties 2. Update with a Feast core
   * (which contains JDBC_SNOWFLAKE STORE) image in docker-compose-it-core.yml
   */
  static final String CORE = "core_1";

  static final int CORE_START_MAX_WAIT_TIME_IN_MINUTES = 3;
  static final int FEAST_CORE_PORT = 6565;
  static final int FEAST_SERVING_PORT = 6566;
  static final int MAX_AGE_SECOND = 30;
  static final String PROJECT_NAME = "myproject4";
  static final String FEATURE_SET = "feature_set";
  static final String ENTITY_NAME = "ENTITY_ID_PRIMARY";
  static final String TEST_FEATURE_1 = "FEATURE_1";
  static final String kafkaTopic = "feast-features";

  @DynamicPropertySource
  static void initialize(DynamicPropertyRegistry registry) throws UnknownHostException {}

  @ClassRule @Container
  public static DockerComposeContainer environment =
      new DockerComposeContainer(
              new File("src/test/resources/docker-compose/docker-compose-it-core.yml"))
          .withExposedService(
              CORE,
              6565,
              Wait.forLogMessage(".*gRPC Server started.*\\n", 1)
                  .withStartupTimeout(Duration.ofMinutes(CORE_START_MAX_WAIT_TIME_IN_MINUTES)));

  @BeforeAll
  static void globalSetup() throws IOException, InitializationError, InterruptedException {
    System.out.print("global setup");
  }

  public KafkaTemplate<String, FeatureRow> specKafkaTemplate() {

    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092,localhost:9094");

    KafkaTemplate<String, FeatureRow> t =
        new KafkaTemplate<>(
            new DefaultKafkaProducerFactory<>(
                props, new StringSerializer(), new KafkaSerialization.ProtoSerializer<>()));
    t.setDefaultTopic(kafkaTopic);
    return t;
  }

  @Test
  public void shouldRetrieveFromSnowflakeTest3DatesWithMaxAge()
      throws IOException, InterruptedException, ParseException {

    String entitySourceUri = "s3://feast-snowflake-staging/test/entity_tables/entities_3dates.csv";
    CoreSimpleAPIClient coreClient = IntegrationTestUtils.getApiClientForCore(FEAST_CORE_PORT);
    IntegrationTestUtils.applyFeatureSet(
        coreClient, PROJECT_NAME, FEATURE_SET, ENTITY_NAME, TEST_FEATURE_1, MAX_AGE_SECOND);
    List<FeatureRow> features = IntegrationTestUtils.ingestFeatures(PROJECT_NAME, FEATURE_SET);
    KafkaTemplate<String, FeatureRow> kafkaTemplate = specKafkaTemplate();
    for (int i = 0; i < features.size(); i++) {
      kafkaTemplate.send(kafkaTopic, features.get(i));
    }

    TimeUnit.MINUTES.sleep(2);
    // Run getBatchFeatures on Serving
    ServingServiceBlockingStub servingStub =
        IntegrationTestUtils.getServingServiceStub(false, FEAST_SERVING_PORT, null);
    GetBatchFeaturesRequest getBatchFeaturesRequest =
        IntegrationTestUtils.createGetBatchFeaturesRequest(
            entitySourceUri, TEST_FEATURE_1, FEATURE_SET, PROJECT_NAME);
    GetBatchFeaturesResponse response = servingStub.getBatchFeatures(getBatchFeaturesRequest);
    Job resultJob = response.getJob();
    GetJobRequest jobRequest = GetJobRequest.newBuilder().setJob(resultJob).build();
    servingStub.getJob(jobRequest).getJob();
    waitAtMost(2, TimeUnit.MINUTES)
        .until(
            () -> servingStub.getJob(jobRequest).getJob(),
            hasProperty("status", equalTo(JobStatus.JOB_STATUS_DONE)));
    resultJob = servingStub.getJob(jobRequest).getJob();
    ProtocolStringList resultUris = resultJob.getFileUrisList();

    // get csv.gz file from s3
    List<String> resultLines = IntegrationTestUtils.readFromS3(resultUris.get(0));
    Assert.assertEquals("\\" + "\\N", resultLines.get(1).split(",")[3]);
    Assert.assertEquals("100", resultLines.get(2).split(",")[3]);
    Assert.assertEquals("300", resultLines.get(3).split(",")[3]);
  }

  @Test
  public void shouldRetrieveFromSnowflakeTest3Dates()
      throws IOException, InterruptedException, ParseException {

    String entitySourceUri = "s3://feast-snowflake-staging/test/entity_tables/entities_3dates.csv";
    CoreSimpleAPIClient coreClient = IntegrationTestUtils.getApiClientForCore(FEAST_CORE_PORT);
    IntegrationTestUtils.applyFeatureSet(
        coreClient, PROJECT_NAME, FEATURE_SET, ENTITY_NAME, TEST_FEATURE_1, null);
    List<FeatureRow> features = IntegrationTestUtils.ingestFeatures(PROJECT_NAME, FEATURE_SET);
    KafkaTemplate<String, FeatureRow> kafkaTemplate = specKafkaTemplate();
    for (int i = 0; i < features.size(); i++) {
      kafkaTemplate.send(kafkaTopic, features.get(i));
    }

    TimeUnit.MINUTES.sleep(2);
    // Run getBatchFeatures on Serving
    ServingServiceBlockingStub servingStub =
        IntegrationTestUtils.getServingServiceStub(false, FEAST_SERVING_PORT, null);
    GetBatchFeaturesRequest getBatchFeaturesRequest =
        IntegrationTestUtils.createGetBatchFeaturesRequest(
            entitySourceUri, TEST_FEATURE_1, FEATURE_SET, PROJECT_NAME);
    GetBatchFeaturesResponse response = servingStub.getBatchFeatures(getBatchFeaturesRequest);
    Job resultJob = response.getJob();
    GetJobRequest jobRequest = GetJobRequest.newBuilder().setJob(resultJob).build();
    servingStub.getJob(jobRequest).getJob();
    waitAtMost(2, TimeUnit.MINUTES)
        .until(
            () -> servingStub.getJob(jobRequest).getJob(),
            hasProperty("status", equalTo(JobStatus.JOB_STATUS_DONE)));
    resultJob = servingStub.getJob(jobRequest).getJob();
    ProtocolStringList resultUris = resultJob.getFileUrisList();

    // get csv.gz file from s3
    List<String> resultLines = IntegrationTestUtils.readFromS3(resultUris.get(0));
    Assert.assertEquals("400", resultLines.get(1).split(",")[3]);
    Assert.assertEquals("100", resultLines.get(2).split(",")[3]);
    Assert.assertEquals("300", resultLines.get(3).split(",")[3]);
  }

  @Test
  public void shouldNotRetrieveFromSnowflakeNotRegisterFeature() {

    String entitySourceUri = "s3://feast-snowflake-staging/test/entity_tables/entities_3dates.csv";
    ServingServiceBlockingStub servingStub =
        IntegrationTestUtils.getServingServiceStub(false, FEAST_SERVING_PORT, null);
    GetBatchFeaturesRequest getBatchFeaturesRequest =
        IntegrationTestUtils.createGetBatchFeaturesRequest(
            entitySourceUri, TEST_FEATURE_1, FEATURE_SET, PROJECT_NAME);

    String expectedErrorMsg =
        "NOT_FOUND: Unable to find Feature Set for the given Feature Reference: myproject4/feature_set:FEATURE_1";
    Exception exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> {
              servingStub.getBatchFeatures(getBatchFeaturesRequest);
            });

    String actualMessage = exception.getMessage();
    assertEquals(actualMessage, expectedErrorMsg);
  }
}
