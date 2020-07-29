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
package feast.core.controller;

import feast.core.it.BaseIT;
import feast.core.it.DataGenerator;
import feast.core.it.SimpleAPIClient;
import feast.core.model.Project;
import feast.proto.core.CoreServiceGrpc;
import feast.proto.core.FeatureSetProto.FeatureSet;
import feast.proto.types.ValueProto.ValueType.Enum;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.List;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.gson.JsonElement;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.gson.JsonParser;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.util.UriComponentsBuilder;

@SpringBootTest
@AutoConfigureWebTestClient
public class CoreServiceRestIT extends BaseIT {

  static CoreServiceGrpc.CoreServiceBlockingStub stub;
  static SimpleAPIClient apiClient;
  @Autowired private WebTestClient webTestClient;

  @BeforeAll
  public static void globalSetUp(@Value("${grpc.server.port}") int port) {
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
    stub = CoreServiceGrpc.newBlockingStub(channel);
    apiClient = new SimpleAPIClient(stub);
  }

  @BeforeEach
  private void createFakeFeatureSets() {
    // spec:
    //  name: merchant_ratings
    //  entities:
    //    - name: merchant_id
    //      valueType: STRING
    //  features:
    //    - name: average_rating
    //      valueType: DOUBLE
    //    - name: total_ratings
    //      valueType: INT64
    //  project: default
    FeatureSet merchantFeatureSet =
        DataGenerator.createFeatureSet(
            DataGenerator.getDefaultSource(),
            Project.DEFAULT_NAME,
            "merchant_ratings",
            List.of(Pair.of("merchant_id", Enum.STRING)),
            List.of(Pair.of("average_rating", Enum.DOUBLE), Pair.of("total_ratings", Enum.INT64)));
    apiClient.simpleApplyFeatureSet(merchantFeatureSet);

    // spec:
    //  name: another_merchant_ratings
    //  entities:
    //    - name: merchant_id
    //      valueType: STRING
    //  features:
    //    - name: another_average_rating
    //      valueType: DOUBLE
    //    - name: another_total_ratings
    //      valueType: INT64
    //  project: default
    FeatureSet anotherMerchantFeatureSet =
        DataGenerator.createFeatureSet(
            DataGenerator.getDefaultSource(),
            Project.DEFAULT_NAME,
            "another_merchant_ratings",
            List.of(Pair.of("merchant_id", Enum.STRING)),
            List.of(
                Pair.of("another_average_rating", Enum.DOUBLE),
                Pair.of("another_total_ratings", Enum.INT64)));
    apiClient.simpleApplyFeatureSet(anotherMerchantFeatureSet);

    // spec:
    //  name: yet_another_merchant_feature_set
    //  entities:
    //    - name: merchant_id
    //      valueType: STRING
    //  features:
    //    - name: merchant_prop1
    //      valueType: BOOL
    //    - name: merchant_prop2
    //      valueType: FLOAT
    //  project: merchant
    FeatureSet yetAnotherMerchantFeatureSet =
        DataGenerator.createFeatureSet(
            DataGenerator.getDefaultSource(),
            "merchant",
            "yet_another_merchant_feature_set",
            List.of(Pair.of("merchant_id", Enum.STRING)),
            List.of(Pair.of("merchant_prop1", Enum.BOOL), Pair.of("merchant_prop2", Enum.FLOAT)));
    apiClient.simpleApplyFeatureSet(yetAnotherMerchantFeatureSet);
  }

  @Test
  public void getVersion() {
    String uriString = UriComponentsBuilder.fromPath("/api/v1/version").toUriString();
    webTestClient
        .get()
        .uri(uriString)
        .exchange()
        .expectStatus()
        .isOk()
        .expectHeader()
        .contentType(MediaType.APPLICATION_JSON)
        .expectBody()
        .jsonPath("$.version")
        .isNotEmpty();
  }

  // list projects
  @Test
  public void listProjects(@Autowired WebTestClient webTestClient) {
    // should get 2 projects
    String uriString = UriComponentsBuilder.fromPath("/api/v1/projects").toUriString();
    webTestClient
        .get()
        .uri(uriString)
        .exchange()
        .expectStatus()
        .isOk()
        .expectHeader()
        .contentType(MediaType.APPLICATION_JSON)
        .expectBody()
        .jsonPath("$.projects")
        .isArray()
        .jsonPath("$.projects")
        .value(
            projects -> {
              JsonElement jsonElement = JsonParser.parseString((String) projects);
              Assert.assertEquals(jsonElement.getAsJsonArray().size(), 2);
            });
  }

  // list feature sets
  @Test
  public void listFeatureSets(@Autowired WebTestClient webTestClient) throws Exception {
    // project = default
    // name = merchant_ratings
    // getting a specific feature set
    String uri1 =
        UriComponentsBuilder.fromPath("/api/v1/feature-sets")
            .queryParam("project", "default")
            .queryParam("name", "merchant_ratings")
            .buildAndExpand()
            .toString();
    webTestClient
        .get()
        .uri(uri1)
        .exchange()
        .expectStatus()
        .isOk()
        .expectHeader()
        .contentType(MediaType.APPLICATION_JSON)
        .expectBody()
        .jsonPath("$.featureSets")
        .isArray()
        .jsonPath("$.featureSets")
        .value(
            projects -> {
              JsonElement jsonElement = JsonParser.parseString((String) projects);
              Assert.assertEquals(jsonElement.getAsJsonArray().size(), 1);
            });

    // project = *
    // name = *merchant_ratings
    // should have two feature sets named *merchant_ratings
    String uri2 =
        UriComponentsBuilder.fromPath("/api/v1/feature-sets")
            .queryParam("project", "*")
            .queryParam("name", "*merchant_ratings")
            .buildAndExpand()
            .toString();
    webTestClient
        .get()
        .uri(uri2)
        .exchange()
        .expectStatus()
        .isOk()
        .expectHeader()
        .contentType(MediaType.APPLICATION_JSON)
        .expectBody()
        .jsonPath("$.featureSets")
        .isArray()
        .jsonPath("$.featureSets")
        .value(
            projects -> {
              System.out.println("here");
              JsonElement jsonElement = JsonParser.parseString((String) projects);
              Assert.assertEquals(jsonElement.getAsJsonArray().size(), 2);
            });

    // project = *
    // name = *
    // should have three feature sets
    String uri3 =
        UriComponentsBuilder.fromPath("/api/v1/feature-sets")
            .queryParam("project", "*")
            .queryParam("name", "*")
            .buildAndExpand()
            .toString();
    webTestClient
        .get()
        .uri(uri3)
        .exchange()
        .expectStatus()
        .isOk()
        .expectHeader()
        .contentType(MediaType.APPLICATION_JSON)
        .expectBody()
        .jsonPath("$.featureSets")
        .isArray()
        .jsonPath("$.featureSets")
        .value(
            projects -> {
              JsonElement jsonElement = JsonParser.parseString((String) projects);
              Assert.assertEquals(jsonElement.getAsJsonArray().size(), 3);
            });
  }

  @Test
  public void listFeatures(@Autowired WebTestClient webTestClient) throws Exception {
    // entities = [merchant_id]
    // project = default
    // should return 4 features
    String uri1 =
        UriComponentsBuilder.fromPath("/api/v1/features")
            .queryParam("entities", "merchant_id")
            .buildAndExpand()
            .toString();
    webTestClient
        .get()
        .uri(uri1)
        .exchange()
        .expectStatus()
        .isOk()
        .expectHeader()
        .contentType(MediaType.APPLICATION_JSON)
        .expectBody()
        .jsonPath("$.features")
        .isMap();

    // entities = [merchant_id]
    // project = merchant
    // should return 2 features
    String url2 =
        UriComponentsBuilder.fromPath("/api/v1/features")
            .queryParam("entities", "merchant_id")
            .queryParam("project", "merchant")
            .buildAndExpand()
            .toString();
    webTestClient
        .get()
        .uri(uri1)
        .exchange()
        .expectStatus()
        .isOk()
        .expectHeader()
        .contentType(MediaType.APPLICATION_JSON)
        .expectBody()
        .jsonPath("$.features")
        .isMap();
  }

  @TestConfiguration
  public static class TestConfig extends BaseTestConfig {}
}
