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
package feast.core.service;

import static com.jayway.jsonassert.impl.matcher.IsMapContainingKey.hasKey;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.beans.HasPropertyWithValue.hasProperty;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsMapWithSize.aMapWithSize;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsIterableContaining.hasItem;

import avro.shaded.com.google.common.collect.ImmutableMap;
import feast.core.it.BaseIT;
import feast.core.it.DataGenerator;
import feast.core.it.SimpleAPIClient;
import feast.proto.core.CoreServiceGrpc;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.SourceProto;
import feast.proto.types.ValueProto;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

@SpringBootTest
public class SpecServiceIT extends BaseIT {

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
  public void initState() {
    SourceProto.Source source = DataGenerator.getDefaultSource();

    apiClient.simpleApplyFeatureSet(
        DataGenerator.createFeatureSet(
            source,
            "default",
            "fs1",
            ImmutableMap.of("id", ValueProto.ValueType.Enum.STRING),
            ImmutableMap.of("total", ValueProto.ValueType.Enum.INT64)));
    apiClient.simpleApplyFeatureSet(
        DataGenerator.createFeatureSet(
            source,
            "default",
            "fs2",
            ImmutableMap.of("user_id", ValueProto.ValueType.Enum.STRING),
            ImmutableMap.of("sum", ValueProto.ValueType.Enum.INT64)));
    apiClient.simpleApplyFeatureSet(
        DataGenerator.createFeatureSet(
            source,
            "project1",
            "fs3",
            ImmutableList.of(
                DataGenerator.createEntity("user_id", ValueProto.ValueType.Enum.STRING)),
            ImmutableList.of(
                DataGenerator.createFeature(
                    "feature1", ValueProto.ValueType.Enum.INT32, Collections.emptyMap()),
                DataGenerator.createFeature(
                    "feature2", ValueProto.ValueType.Enum.INT32, Collections.emptyMap())),
            Collections.emptyMap()));
    apiClient.simpleApplyFeatureSet(
        DataGenerator.createFeatureSet(
            source,
            "project1",
            "fs4",
            ImmutableList.of(
                DataGenerator.createEntity("customer_id", ValueProto.ValueType.Enum.STRING)),
            ImmutableList.of(
                DataGenerator.createFeature(
                    "feature2",
                    ValueProto.ValueType.Enum.INT32,
                    ImmutableMap.of("app", "feast", "version", "one"))),
            ImmutableMap.of("label", "some")));
    apiClient.simpleApplyFeatureSet(
        DataGenerator.createFeatureSet(
            source,
            "project1",
            "fs5",
            ImmutableList.of(
                DataGenerator.createEntity("customer_id", ValueProto.ValueType.Enum.STRING)),
            ImmutableList.of(
                DataGenerator.createFeature(
                    "feature3",
                    ValueProto.ValueType.Enum.INT32,
                    ImmutableMap.of("app", "feast", "version", "two"))),
            Collections.emptyMap()));
    apiClient.simpleApplyFeatureSet(DataGenerator.createFeatureSet(source, "default", "new_fs"));
  }

  @Nested
  class ListFeatureSets {

    @Test
    public void shouldGetAllFeatureSetsIfOnlyWildcardsProvided() {
      List<FeatureSetProto.FeatureSet> featureSets = apiClient.simpleListFeatureSets("*", "*");

      assertThat(featureSets, hasSize(6));
    }

    @Test
    public void shouldGetAllFeatureSetsMatchingNameWithWildcardSearch() {
      List<FeatureSetProto.FeatureSet> featureSets =
          apiClient.simpleListFeatureSets("default", "fs*");

      assertThat(featureSets, hasSize(2));
      assertThat(featureSets, hasItem(hasProperty("spec", hasProperty("name", equalTo("fs1")))));
      assertThat(featureSets, hasItem(hasProperty("spec", hasProperty("name", equalTo("fs2")))));
    }

    @Test
    public void shouldFilterFeatureSetsByNameAndProject() {
      List<FeatureSetProto.FeatureSet> featureSets =
          apiClient.simpleListFeatureSets("project1", "fs3");

      assertThat(featureSets, hasItem(hasProperty("spec", hasProperty("name", equalTo("fs3")))));
    }

    @Test
    public void shouldFilterFeatureSetsByStatus() {
      apiClient.updateFeatureSetStatus(
          "project1", "fs3", FeatureSetProto.FeatureSetStatus.STATUS_READY);

      apiClient.updateFeatureSetStatus(
          "project1", "fs4", FeatureSetProto.FeatureSetStatus.STATUS_READY);

      List<FeatureSetProto.FeatureSet> featureSets =
          apiClient.simpleListFeatureSets("*", "*", FeatureSetProto.FeatureSetStatus.STATUS_READY);

      assertThat(featureSets, hasSize(2));
      assertThat(featureSets, hasItem(hasProperty("spec", hasProperty("name", equalTo("fs3")))));
      assertThat(featureSets, hasItem(hasProperty("spec", hasProperty("name", equalTo("fs4")))));

      assertThat(
          apiClient.simpleListFeatureSets(
              "default", "*", FeatureSetProto.FeatureSetStatus.STATUS_PENDING),
          hasSize(3));
    }

    @Test
    public void shouldFilterFeatureSetsByLabels() {
      List<FeatureSetProto.FeatureSet> featureSets =
          apiClient.simpleListFeatureSets("project1", "*", ImmutableMap.of("label", "some"));

      assertThat(featureSets, hasSize(1));
      assertThat(featureSets, hasItem(hasProperty("spec", hasProperty("name", equalTo("fs4")))));
    }

    @Test
    public void shouldUseDefaultProjectIfProjectUnspecified() {
      List<FeatureSetProto.FeatureSet> featureSets = apiClient.simpleListFeatureSets("", "*");

      assertThat(featureSets, hasSize(3));
      assertThat(featureSets, hasItem(hasProperty("spec", hasProperty("name", equalTo("fs1")))));
      assertThat(featureSets, hasItem(hasProperty("spec", hasProperty("name", equalTo("fs2")))));
      assertThat(featureSets, hasItem(hasProperty("spec", hasProperty("name", equalTo("new_fs")))));
    }

    @Test
    public void shouldThrowExceptionGivenMissingFeatureSetName() {}
  }

  @Test
  public void shouldFilterFeaturesByEntitiesAndLabels() {
    // Case 1: Only filter by entities
    Map<String, FeatureSetProto.FeatureSpec> result1 =
        apiClient.simpleListFeatures("project1", "user_id");

    assertThat(result1, aMapWithSize(2));
    assertThat(result1, hasKey(equalTo("project1/fs3:feature1")));
    assertThat(result1, hasKey(equalTo("project1/fs3:feature2")));

    // Case 2: Filter by entities and labels
    Map<String, FeatureSetProto.FeatureSpec> result2 =
        apiClient.simpleListFeatures(
            "project1",
            ImmutableMap.of("app", "feast", "version", "one"),
            ImmutableList.of("customer_id"));

    assertThat(result2, aMapWithSize(1));
    assertThat(result2, hasKey(equalTo("project1/fs4:feature2")));

    // Case 3: Filter by labels
    Map<String, FeatureSetProto.FeatureSpec> result3 =
        apiClient.simpleListFeatures(
            "project1", ImmutableMap.of("app", "feast"), Collections.emptyList());

    assertThat(result3, aMapWithSize(2));
    assertThat(result3, hasKey(equalTo("project1/fs4:feature2")));
    assertThat(result3, hasKey(equalTo("project1/fs5:feature3")));

    // Case 4: Filter by nothing, except project
    Map<String, FeatureSetProto.FeatureSpec> result4 =
        apiClient.simpleListFeatures("project1", ImmutableMap.of(), Collections.emptyList());

    assertThat(result4, aMapWithSize(4));
    assertThat(result4, hasKey(equalTo("project1/fs3:feature1")));
    assertThat(result4, hasKey(equalTo("project1/fs3:feature1")));
    assertThat(result4, hasKey(equalTo("project1/fs4:feature2")));
    assertThat(result4, hasKey(equalTo("project1/fs5:feature3")));

    // Case 5: Filter by nothing; will use default project
    Map<String, FeatureSetProto.FeatureSpec> result5 =
        apiClient.simpleListFeatures("", ImmutableMap.of(), Collections.emptyList());

    assertThat(result5, aMapWithSize(2));
    assertThat(result5, hasKey(equalTo("default/fs1:total")));
    assertThat(result5, hasKey(equalTo("default/fs2:sum")));
  }
}
