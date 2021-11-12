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

import static io.restassured.RestAssured.get;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableMap;
import feast.common.it.BaseIT;
import feast.common.it.DataGenerator;
import feast.common.it.SimpleCoreClient;
import feast.proto.core.CoreServiceGrpc;
import feast.proto.core.EntityProto;
import feast.proto.core.FeatureTableProto;
import feast.proto.types.ValueProto;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.path.json.JsonPath;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.web.util.UriComponentsBuilder;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
public class CoreServiceRestIT extends BaseIT {

  static CoreServiceGrpc.CoreServiceBlockingStub stub;
  static SimpleCoreClient apiClient;
  @LocalServerPort private int port;

  @TestConfiguration
  public static class TestConfig extends BaseTestConfig {}

  @BeforeAll
  public static void globalSetUp(@Value("${grpc.server.port}") int port) {
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
    stub = CoreServiceGrpc.newBlockingStub(channel);
    apiClient = new SimpleCoreClient(stub);
  }

  @Test
  public void getVersion() {
    String uriString = UriComponentsBuilder.fromPath("/api/v2/version").toUriString();
    get(uriString)
        .then()
        .log()
        .everything()
        .assertThat()
        .contentType(ContentType.JSON)
        .body("version", notNullValue());
  }

  // list projects
  @Test
  public void listProjects() {
    // should get 2 projects
    String uriString = UriComponentsBuilder.fromPath("/api/v2/projects").toUriString();
    String responseBody =
        get(uriString)
            .then()
            .log()
            .everything()
            .assertThat()
            .contentType(ContentType.JSON)
            .extract()
            .response()
            .getBody()
            .asString();
    List<String> projectList = JsonPath.from(responseBody).getList("projects");
    assertEquals(projectList, List.of("default"));
  }

  @Test
  public void listFeatures() {
    String uri1 =
        UriComponentsBuilder.fromPath("/api/v2/features")
            .queryParam("entities", "entity1", "entity2")
            .buildAndExpand()
            .toString();
    get(uri1)
        .then()
        .log()
        .everything()
        .assertThat()
        .contentType(ContentType.JSON)
        .body("features", aMapWithSize(2));

    String uri2 =
        UriComponentsBuilder.fromPath("/api/v2/features")
            .queryParam("entities", "entity1", "entity2")
            .queryParam("project", "default")
            .buildAndExpand()
            .toString();
    get(uri2)
        .then()
        .log()
        .everything()
        .assertThat()
        .contentType(ContentType.JSON)
        .body("features", aMapWithSize(2));
  }

  @Test
  public void listEntities() {
    String uri1 =
        UriComponentsBuilder.fromPath("/api/v2/entities")
            .queryParam("project", "default")
            .buildAndExpand()
            .toString();
    String responseBody =
        get(uri1)
            .then()
            .log()
            .everything()
            .assertThat()
            .contentType(ContentType.JSON)
            .extract()
            .response()
            .getBody()
            .asString();
    List<String> entityList = JsonPath.from(responseBody).getList("entities");
    assertEquals(entityList.size(), 2);
  }

  @Test
  public void listFeatureTables() {
    String uri1 =
        UriComponentsBuilder.fromPath("/api/v2/feature-tables")
            .queryParam("project", "default")
            .buildAndExpand()
            .toString();
    String responseBody =
        get(uri1)
            .then()
            .log()
            .everything()
            .assertThat()
            .contentType(ContentType.JSON)
            .extract()
            .response()
            .getBody()
            .asString();
    List<String> featureTableList = JsonPath.from(responseBody).getList("tables");
    assertEquals(featureTableList.size(), 1);
  }

  @BeforeEach
  private void createSpecs() {
    // Apply entities
    EntityProto.EntitySpecV2 entitySpec1 =
        DataGenerator.createEntitySpecV2(
            "entity1",
            "Entity 1 description",
            ValueProto.ValueType.Enum.STRING,
            avro.shaded.com.google.common.collect.ImmutableMap.of("label_key", "label_value"));
    EntityProto.EntitySpecV2 entitySpec2 =
        DataGenerator.createEntitySpecV2(
            "entity2",
            "Entity 2 description",
            ValueProto.ValueType.Enum.STRING,
            avro.shaded.com.google.common.collect.ImmutableMap.of("label_key2", "label_value2"));
    apiClient.simpleApplyEntity("default", entitySpec1);
    apiClient.simpleApplyEntity("default", entitySpec2);

    // Apply feature table
    FeatureTableProto.FeatureTableSpec featureTableSpec =
        DataGenerator.createFeatureTableSpec(
                "featuretable1",
                Arrays.asList("entity1", "entity2"),
                new HashMap<>() {
                  {
                    put("feature1", ValueProto.ValueType.Enum.STRING);
                    put("feature2", ValueProto.ValueType.Enum.FLOAT);
                  }
                },
                7200,
                ImmutableMap.of("feat_key2", "feat_value2"))
            .toBuilder()
            .setBatchSource(
                DataGenerator.createFileDataSourceSpec("file:///path/to/file", "ts_col", ""))
            .build();
    apiClient.applyFeatureTable("default", featureTableSpec);

    RestAssured.port = port;
  }
}
