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

import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

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
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.util.UriComponentsBuilder;

@SpringBootTest
@AutoConfigureMockMvc
public class CoreServiceRestIT extends BaseIT {

  @Autowired private WebApplicationContext webApplicationContext;
  @Autowired private MockMvc mockMvc;

  static CoreServiceGrpc.CoreServiceBlockingStub stub;
  static SimpleAPIClient apiClient;

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
  public void getVersion() throws Exception {
    Assert.assertNotNull(mockMvc);
    String url = UriComponentsBuilder.fromPath("/api/v1/version").buildAndExpand().toString();
    mockMvc
        .perform(MockMvcRequestBuilders.get(url).accept(MediaType.APPLICATION_JSON))
        .andDo(print())
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.version").isNotEmpty());
  }

  // list projects
  @Test
  public void listProjects() throws Exception {
    // should get 2 projects
    String url = UriComponentsBuilder.fromPath("/api/v1/projects").buildAndExpand().toString();
    mockMvc
        .perform(MockMvcRequestBuilders.get(url).accept(MediaType.APPLICATION_JSON))
        .andDo(print())
        .andExpect(status().isOk());
  }

  // list feature sets
  @Test
  public void listFeatureSets() throws Exception {
    // project = default
    // name = merchant_ratings
    // getting a specific feature set
    String url1 =
        UriComponentsBuilder.fromPath("/api/v1/feature-sets")
            .queryParam("project", "default")
            .queryParam("name", "merchant_ratings")
            .buildAndExpand()
            .toString();
    mockMvc
        .perform(MockMvcRequestBuilders.get(url1).accept(MediaType.APPLICATION_JSON))
        .andDo(print())
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.featureSets").isNotEmpty());

    // project = *
    // name = *merchant_ratings
    // should have two feature sets named *merchant_ratings
    String url2 =
        UriComponentsBuilder.fromPath("/api/v1/feature-sets")
            .queryParam("project", "*")
            .queryParam("name", "*merchant_ratings")
            .buildAndExpand()
            .toString();
    mockMvc
        .perform(MockMvcRequestBuilders.get(url2).accept(MediaType.APPLICATION_JSON))
        .andDo(print())
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.featureSets").isNotEmpty());

    // project = *
    // name = *
    // should have three feature sets
    String url3 =
        UriComponentsBuilder.fromPath("/api/v1/feature-sets")
            .queryParam("project", "*")
            .queryParam("name", "*")
            .buildAndExpand()
            .toString();
    mockMvc
        .perform(MockMvcRequestBuilders.get(url3).accept(MediaType.APPLICATION_JSON))
        .andDo(print())
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.featureSets").isArray());
  }

  @Test
  public void listFeatures() throws Exception {
    // entities = [merchant_id]
    // project = default
    // should return 4 features
    String url1 =
        UriComponentsBuilder.fromPath("/api/v1/features")
            .queryParam("entities", "merchant_id")
            .buildAndExpand()
            .toString();
    mockMvc
        .perform(MockMvcRequestBuilders.get(url1).accept(MediaType.APPLICATION_JSON))
        .andDo(print())
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.features").isMap());

    // entities = [merchant_id]
    // project = merchant
    // should return 2 features
    String url2 =
        UriComponentsBuilder.fromPath("/api/v1/features")
            .queryParam("entities", "merchant_id")
            .queryParam("project", "merchant")
            .buildAndExpand()
            .toString();
    mockMvc
        .perform(MockMvcRequestBuilders.get(url2).accept(MediaType.APPLICATION_JSON))
        .andDo(print())
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.features").isMap());
  }

  @TestConfiguration
  public static class TestConfig extends BaseTestConfig {}
}
