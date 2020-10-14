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
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.beans.HasPropertyWithValue.hasProperty;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.hamcrest.collection.IsMapWithSize.aMapWithSize;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsIterableContaining.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import avro.shaded.com.google.common.collect.ImmutableMap;
import com.google.protobuf.Duration;
import feast.common.it.BaseIT;
import feast.common.it.DataGenerator;
import feast.common.it.SimpleCoreClient;
import feast.common.util.TestUtil;
import feast.proto.core.*;
import feast.proto.core.FeatureTableProto.FeatureTableSpec;
import feast.proto.types.ValueProto;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.util.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.tensorflow.metadata.v0.*;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

@SpringBootTest
public class SpecServiceIT extends BaseIT {

  static CoreServiceGrpc.CoreServiceBlockingStub stub;
  static SimpleCoreClient apiClient;

  @BeforeAll
  public static void globalSetUp(@Value("${grpc.server.port}") int port) {
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
    stub = CoreServiceGrpc.newBlockingStub(channel);
    apiClient = new SimpleCoreClient(stub);
  }

  @BeforeEach
  public void initState() {
    SourceProto.Source source = DataGenerator.getDefaultSource();

    EntityProto.EntitySpecV2 entitySpec1 =
        DataGenerator.createEntitySpecV2(
            "entity1",
            "Entity 1 description",
            ValueProto.ValueType.Enum.STRING,
            ImmutableMap.of("label_key", "label_value"));
    EntityProto.EntitySpecV2 entitySpec2 =
        DataGenerator.createEntitySpecV2(
            "entity2",
            "Entity 2 description",
            ValueProto.ValueType.Enum.STRING,
            ImmutableMap.of("label_key2", "label_value2"));
    apiClient.simpleApplyEntity("default", entitySpec1);
    apiClient.simpleApplyEntity("default", entitySpec2);
    apiClient.applyFeatureTable(
        "default",
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
                DataGenerator.createFileDataSourceSpec(
                    "file:///path/to/file", "ts_col", ""))
            .build());
    apiClient.simpleApplyEntity(
        "project1",
        DataGenerator.createEntitySpecV2(
            "entity3",
            "Entity 3 description",
            ValueProto.ValueType.Enum.STRING,
            ImmutableMap.of("label_key2", "label_value2")));
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
                DataGenerator.createEntitySpec("user_id", ValueProto.ValueType.Enum.STRING)),
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
                DataGenerator.createEntitySpec("customer_id", ValueProto.ValueType.Enum.STRING)),
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
                DataGenerator.createEntitySpec("customer_id", ValueProto.ValueType.Enum.STRING)),
            ImmutableList.of(
                DataGenerator.createFeature(
                    "feature3",
                    ValueProto.ValueType.Enum.INT32,
                    ImmutableMap.of("app", "feast", "version", "two"))),
            Collections.emptyMap()));
    apiClient.simpleApplyFeatureSet(DataGenerator.createFeatureSet(source, "default", "new_fs"));
    apiClient.updateStore(DataGenerator.getDefaultStore());
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
    public void shouldThrowExceptionGivenMissingFeatureSetName() {
      assertThrows(StatusRuntimeException.class, () -> apiClient.simpleListFeatureSets("", ""));
    }
  }

  @Nested
  class ListEntities {
    @Test
    public void shouldFilterEntitiesByLabels() {
      List<EntityProto.Entity> entities =
          apiClient.simpleListEntities("", ImmutableMap.of("label_key2", "label_value2"));

      assertThat(entities, hasSize(1));
      assertThat(entities, hasItem(hasProperty("spec", hasProperty("name", equalTo("entity2")))));
    }

    @Test
    public void shouldUseDefaultProjectIfProjectUnspecified() {
      List<EntityProto.Entity> entities = apiClient.simpleListEntities("");

      assertThat(entities, hasSize(2));
      assertThat(entities, hasItem(hasProperty("spec", hasProperty("name", equalTo("entity1")))));
    }

    @Test
    public void shouldFilterEntitiesByProjectAndLabels() {
      List<EntityProto.Entity> entities =
          apiClient.simpleListEntities("project1", ImmutableMap.of("label_key2", "label_value2"));

      assertThat(entities, hasSize(1));
      assertThat(entities, hasItem(hasProperty("spec", hasProperty("name", equalTo("entity3")))));
    }

    @Test
    public void shouldThrowExceptionGivenWildcardProject() {
      CoreServiceProto.ListEntitiesRequest.Filter filter =
          CoreServiceProto.ListEntitiesRequest.Filter.newBuilder().setProject("default*").build();
      StatusRuntimeException exc =
          assertThrows(StatusRuntimeException.class, () -> apiClient.simpleListEntities(filter));

      assertThat(
          exc.getMessage(),
          equalTo(
              String.format(
                  "INVALID_ARGUMENT: invalid value for project resource, %s: "
                      + "argument must only contain alphanumeric characters and underscores.",
                  filter.getProject())));
    }
  }

  @Nested
  class ListFeatureTables {
    @Test
    public void shouldFilterFeatureTablesByProjectAndLabels() {
      CoreServiceProto.ListFeatureTablesRequest.Filter filter =
          CoreServiceProto.ListFeatureTablesRequest.Filter.newBuilder()
              .setProject("default")
              .putAllLabels(ImmutableMap.of("feat_key2", "feat_value2"))
              .build();
      List<FeatureTableProto.FeatureTable> featureTables =
          apiClient.simpleListFeatureTables(filter);

      assertThat(featureTables, hasSize(1));
      assertThat(
          featureTables,
          hasItem(hasProperty("spec", hasProperty("name", equalTo("featuretable1")))));
    }

    @Test
    public void shouldUseDefaultProjectIfProjectUnspecified() {
      CoreServiceProto.ListFeatureTablesRequest.Filter filter =
          CoreServiceProto.ListFeatureTablesRequest.Filter.newBuilder()
              .setProject("default")
              .build();
      List<FeatureTableProto.FeatureTable> featureTables =
          apiClient.simpleListFeatureTables(filter);

      assertThat(featureTables, hasSize(1));
      assertThat(
          featureTables,
          hasItem(hasProperty("spec", hasProperty("name", equalTo("featuretable1")))));
    }

    @Test
    public void shouldThrowExceptionGivenWildcardProject() {
      CoreServiceProto.ListFeatureTablesRequest.Filter filter =
          CoreServiceProto.ListFeatureTablesRequest.Filter.newBuilder()
              .setProject("default*")
              .build();
      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class, () -> apiClient.simpleListFeatureTables(filter));

      assertThat(
          exc.getMessage(),
          equalTo(
              String.format(
                  "INVALID_ARGUMENT: invalid value for project resource, %s: "
                      + "argument must only contain alphanumeric characters and underscores.",
                  filter.getProject())));
    }
  }

  @Nested
  class ApplyFeatureSet {
    @Test
    public void shouldThrowExceptionGivenReservedFeatureName() {
      List<String> reservedNames =
          Arrays.asList("created_timestamp", "event_timestamp", "ingestion_id", "job_id");
      String reservedNamesString = StringUtils.join(reservedNames, ", ");

      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  apiClient.simpleApplyFeatureSet(
                      DataGenerator.createFeatureSet(
                          DataGenerator.getDefaultSource(),
                          "project",
                          "name",
                          ImmutableMap.of("entity", ValueProto.ValueType.Enum.STRING),
                          ImmutableMap.of("event_timestamp", ValueProto.ValueType.Enum.STRING))));

      assertThat(
          exc.getMessage(),
          equalTo(
              String.format(
                  "INTERNAL: Reserved feature names have been used, which are not allowed. These names include %s."
                      + "You've just used an invalid name, %s.",
                  reservedNamesString, "event_timestamp")));
    }

    @Test
    public void shouldThrowExceptionGivenFeatureSetWithDash() {
      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  apiClient.simpleApplyFeatureSet(
                      DataGenerator.createFeatureSet(
                          DataGenerator.getDefaultSource(),
                          "project",
                          "dash-name",
                          ImmutableMap.of("entity", ValueProto.ValueType.Enum.STRING),
                          ImmutableMap.of("test_string", ValueProto.ValueType.Enum.STRING))));

      assertThat(
          exc.getMessage(),
          equalTo(
              String.format(
                  "INTERNAL: invalid value for %s resource, %s: %s",
                  "featureset",
                  "dash-name",
                  "argument must only contain alphanumeric characters and underscores.")));
    }

    @Test
    public void shouldReturnFeatureSetIfFeatureSetHasNotChanged() {
      FeatureSetProto.FeatureSet featureSet = apiClient.getFeatureSet("default", "fs1");

      CoreServiceProto.ApplyFeatureSetResponse response =
          apiClient.simpleApplyFeatureSet(featureSet);

      assertThat(
          response.getStatus(), equalTo(CoreServiceProto.ApplyFeatureSetResponse.Status.NO_CHANGE));
      assertThat(
          response.getFeatureSet().getSpec().getVersion(),
          equalTo(featureSet.getSpec().getVersion()));
    }

    @Test
    public void shouldApplyFeatureSetIfNotExists() {
      FeatureSetProto.FeatureSet featureSet =
          DataGenerator.createFeatureSet(
              DataGenerator.getDefaultSource(),
              "default",
              "new",
              ImmutableMap.of("id", ValueProto.ValueType.Enum.STRING),
              ImmutableMap.of("feature", ValueProto.ValueType.Enum.STRING));

      CoreServiceProto.ApplyFeatureSetResponse response =
          apiClient.simpleApplyFeatureSet(featureSet);

      assertThat(
          response.getFeatureSet().getSpec(),
          equalTo(
              featureSet
                  .getSpec()
                  .toBuilder()
                  .setVersion(1)
                  .setMaxAge(Duration.newBuilder().build())
                  .build()));
      assertThat(
          response.getStatus(), equalTo(CoreServiceProto.ApplyFeatureSetResponse.Status.CREATED));
    }

    @Test
    public void shouldUpdateAndSaveFeatureSetIfAlreadyExists() {
      CoreServiceProto.ApplyFeatureSetResponse response =
          apiClient.simpleApplyFeatureSet(
              DataGenerator.createFeatureSet(
                  DataGenerator.getDefaultSource(),
                  "default",
                  "fs1",
                  ImmutableMap.of("id", ValueProto.ValueType.Enum.STRING),
                  ImmutableMap.of(
                      "total", ValueProto.ValueType.Enum.INT64,
                      "subtotal", ValueProto.ValueType.Enum.INT64)));

      assertThat(
          response.getFeatureSet().getSpec().getFeaturesList(),
          hasItem(hasProperty("name", equalTo("subtotal"))));
      assertThat(
          response.getStatus(), equalTo(CoreServiceProto.ApplyFeatureSetResponse.Status.UPDATED));
      assertThat(response.getFeatureSet().getSpec().getVersion(), equalTo(2));
    }

    @Test
    public void shouldAcceptPresenceShapeAndDomainConstraints() {
      List<FeatureSetProto.EntitySpec> entitySpecs = new ArrayList<>();
      entitySpecs.add(
          FeatureSetProto.EntitySpec.newBuilder()
              .setName("entity1")
              .setValueType(ValueProto.ValueType.Enum.INT64)
              .build());
      entitySpecs.add(
          FeatureSetProto.EntitySpec.newBuilder()
              .setName("entity2")
              .setValueType(ValueProto.ValueType.Enum.INT64)
              .build());
      entitySpecs.add(
          FeatureSetProto.EntitySpec.newBuilder()
              .setName("entity3")
              .setValueType(ValueProto.ValueType.Enum.FLOAT)
              .build());
      entitySpecs.add(
          FeatureSetProto.EntitySpec.newBuilder()
              .setName("entity4")
              .setValueType(ValueProto.ValueType.Enum.STRING)
              .build());
      entitySpecs.add(
          FeatureSetProto.EntitySpec.newBuilder()
              .setName("entity5")
              .setValueType(ValueProto.ValueType.Enum.BOOL)
              .build());

      List<FeatureSetProto.FeatureSpec> featureSpecs = new ArrayList<>();
      featureSpecs.add(
          FeatureSetProto.FeatureSpec.newBuilder()
              .setName("feature1")
              .setValueType(ValueProto.ValueType.Enum.INT64)
              .setPresence(FeaturePresence.getDefaultInstance())
              .setShape(FixedShape.getDefaultInstance())
              .setDomain("mydomain")
              .build());
      featureSpecs.add(
          FeatureSetProto.FeatureSpec.newBuilder()
              .setName("feature2")
              .setValueType(ValueProto.ValueType.Enum.INT64)
              .setGroupPresence(FeaturePresenceWithinGroup.getDefaultInstance())
              .setValueCount(ValueCount.getDefaultInstance())
              .setIntDomain(IntDomain.getDefaultInstance())
              .build());
      featureSpecs.add(
          FeatureSetProto.FeatureSpec.newBuilder()
              .setName("feature3")
              .setValueType(ValueProto.ValueType.Enum.FLOAT)
              .setPresence(FeaturePresence.getDefaultInstance())
              .setValueCount(ValueCount.getDefaultInstance())
              .setFloatDomain(FloatDomain.getDefaultInstance())
              .build());
      featureSpecs.add(
          FeatureSetProto.FeatureSpec.newBuilder()
              .setName("feature4")
              .setValueType(ValueProto.ValueType.Enum.STRING)
              .setPresence(FeaturePresence.getDefaultInstance())
              .setValueCount(ValueCount.getDefaultInstance())
              .setStringDomain(StringDomain.getDefaultInstance())
              .build());
      featureSpecs.add(
          FeatureSetProto.FeatureSpec.newBuilder()
              .setName("feature5")
              .setValueType(ValueProto.ValueType.Enum.BOOL)
              .setPresence(FeaturePresence.getDefaultInstance())
              .setValueCount(ValueCount.getDefaultInstance())
              .setBoolDomain(BoolDomain.getDefaultInstance())
              .build());

      FeatureSetProto.FeatureSetSpec featureSetSpec =
          FeatureSetProto.FeatureSetSpec.newBuilder()
              .setProject("project1")
              .setName("featureSetWithConstraints")
              .addAllEntities(entitySpecs)
              .addAllFeatures(featureSpecs)
              .build();
      FeatureSetProto.FeatureSet featureSet =
          FeatureSetProto.FeatureSet.newBuilder().setSpec(featureSetSpec).build();

      CoreServiceProto.ApplyFeatureSetResponse applyFeatureSetResponse =
          apiClient.simpleApplyFeatureSet(featureSet);
      FeatureSetProto.FeatureSetSpec appliedFeatureSetSpec =
          applyFeatureSetResponse.getFeatureSet().getSpec();

      // appliedEntitySpecs needs to be sorted because the list returned by specService may not
      // follow the order in the request
      List<FeatureSetProto.EntitySpec> appliedEntitySpecs =
          new ArrayList<>(appliedFeatureSetSpec.getEntitiesList());
      appliedEntitySpecs.sort(Comparator.comparing(FeatureSetProto.EntitySpec::getName));

      // appliedFeatureSpecs needs to be sorted because the list returned by specService may not
      // follow the order in the request
      List<FeatureSetProto.FeatureSpec> appliedFeatureSpecs =
          new ArrayList<>(appliedFeatureSetSpec.getFeaturesList());
      appliedFeatureSpecs.sort(Comparator.comparing(FeatureSetProto.FeatureSpec::getName));

      assertEquals(appliedEntitySpecs, entitySpecs);
      assertEquals(appliedFeatureSpecs, featureSpecs);
    }

    @Test
    public void shouldUpdateFeatureSetWhenConstraintsAreUpdated() {
      CoreServiceProto.ApplyFeatureSetResponse response =
          apiClient.simpleApplyFeatureSet(
              DataGenerator.createFeatureSet(
                  DataGenerator.getDefaultSource(),
                  "default",
                  "fs1",
                  ImmutableList.of(
                      FeatureSetProto.EntitySpec.newBuilder()
                          .setName("id")
                          .setValueType(ValueProto.ValueType.Enum.STRING)
                          .build()),
                  ImmutableList.of(
                      FeatureSetProto.FeatureSpec.newBuilder()
                          .setName("total")
                          .setValueType(ValueProto.ValueType.Enum.INT64)
                          .setIntDomain(IntDomain.newBuilder().setMin(0).setMax(100).build())
                          .build()),
                  Collections.emptyMap()));

      assertThat(
          response.getStatus(), equalTo(CoreServiceProto.ApplyFeatureSetResponse.Status.UPDATED));
      assertThat(
          response.getFeatureSet().getSpec().getFeaturesList(),
          hasItem(
              hasProperty(
                  "intDomain", equalTo(IntDomain.newBuilder().setMin(0).setMax(100).build()))));
    }

    @Test
    public void shouldCreateProjectWhenNotAlreadyExists() {
      CoreServiceProto.ApplyFeatureSetResponse response =
          apiClient.simpleApplyFeatureSet(
              DataGenerator.createFeatureSet(
                  DataGenerator.getDefaultSource(),
                  "new_project",
                  "new_fs",
                  ImmutableMap.of("id", ValueProto.ValueType.Enum.STRING),
                  ImmutableMap.of("total", ValueProto.ValueType.Enum.INT64)));

      assertThat(
          response.getStatus(), equalTo(CoreServiceProto.ApplyFeatureSetResponse.Status.CREATED));
      assertThat(response.getFeatureSet().getSpec().getProject(), equalTo("new_project"));
    }

    @Test
    public void shouldUsedDefaultProjectIfUnspecified() {
      CoreServiceProto.ApplyFeatureSetResponse response =
          apiClient.simpleApplyFeatureSet(
              DataGenerator.createFeatureSet(
                  DataGenerator.getDefaultSource(),
                  "",
                  "some",
                  ImmutableMap.of("id", ValueProto.ValueType.Enum.STRING),
                  ImmutableMap.of("total", ValueProto.ValueType.Enum.INT64)));

      assertThat(
          response.getStatus(), equalTo(CoreServiceProto.ApplyFeatureSetResponse.Status.CREATED));
      assertThat(response.getFeatureSet().getSpec().getProject(), equalTo("default"));
    }

    @Test
    public void shouldFailWhenProjectIsArchived() {
      apiClient.createProject("archived");
      apiClient.archiveProject("archived");

      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  apiClient.simpleApplyFeatureSet(
                      DataGenerator.createFeatureSet(
                          DataGenerator.getDefaultSource(),
                          "archived",
                          "fs",
                          ImmutableMap.of("id", ValueProto.ValueType.Enum.STRING),
                          ImmutableMap.of("total", ValueProto.ValueType.Enum.INT64))));
      assertThat(exc.getMessage(), equalTo("INTERNAL: Project is archived: archived"));
    }

    @Test
    public void shouldAcceptFeatureLabels() {
      CoreServiceProto.ApplyFeatureSetResponse response =
          apiClient.simpleApplyFeatureSet(
              DataGenerator.createFeatureSet(
                  DataGenerator.getDefaultSource(),
                  "default",
                  "some",
                  ImmutableList.of(
                      FeatureSetProto.EntitySpec.newBuilder()
                          .setName("id")
                          .setValueType(ValueProto.ValueType.Enum.STRING)
                          .build()),
                  ImmutableList.of(
                      FeatureSetProto.FeatureSpec.newBuilder()
                          .setName("feature1")
                          .setValueType(ValueProto.ValueType.Enum.INT64)
                          .putAllLabels(ImmutableMap.of("type", "integer"))
                          .build(),
                      FeatureSetProto.FeatureSpec.newBuilder()
                          .setName("feature2")
                          .setValueType(ValueProto.ValueType.Enum.STRING)
                          .putAllLabels(ImmutableMap.of("type", "string"))
                          .build()),
                  Collections.emptyMap()));

      assertThat(
          response.getStatus(), equalTo(CoreServiceProto.ApplyFeatureSetResponse.Status.CREATED));
      assertThat(
          response.getFeatureSet().getSpec().getFeaturesList(),
          hasItem(
              allOf(
                  hasProperty("name", equalTo("feature1")),
                  hasProperty("labelsMap", hasEntry("type", "integer")))));
      assertThat(
          response.getFeatureSet().getSpec().getFeaturesList(),
          hasItem(
              allOf(
                  hasProperty("name", equalTo("feature2")),
                  hasProperty("labelsMap", hasEntry("type", "string")))));
    }

    @Test
    public void shouldUpdateLabels() {
      CoreServiceProto.ApplyFeatureSetResponse response =
          apiClient.simpleApplyFeatureSet(
              DataGenerator.createFeatureSet(
                  DataGenerator.getDefaultSource(),
                  "project1",
                  "fs4",
                  ImmutableList.of(
                      DataGenerator.createEntitySpec(
                          "customer_id", ValueProto.ValueType.Enum.STRING)),
                  ImmutableList.of(
                      DataGenerator.createFeature(
                          "feature2",
                          ValueProto.ValueType.Enum.INT32,
                          ImmutableMap.of("app", "feast", "version", "two"))),
                  ImmutableMap.of("label", "some")));

      assertThat(
          response.getStatus(), equalTo(CoreServiceProto.ApplyFeatureSetResponse.Status.UPDATED));
      assertThat(
          response.getFeatureSet().getSpec().getFeaturesList(),
          hasItem(
              allOf(
                  hasProperty("name", equalTo("feature2")),
                  hasProperty("labelsMap", hasEntry("version", "two")))));
    }

    @Test
    public void shouldAcceptFeatureSetLabels() {
      CoreServiceProto.ApplyFeatureSetResponse response =
          apiClient.simpleApplyFeatureSet(
              DataGenerator.createFeatureSet(
                  DataGenerator.getDefaultSource(),
                  "",
                  "some",
                  ImmutableList.of(
                      DataGenerator.createEntitySpec(
                          "customer_id", ValueProto.ValueType.Enum.STRING)),
                  ImmutableList.of(),
                  ImmutableMap.of("label", "some")));

      assertThat(
          response.getStatus(), equalTo(CoreServiceProto.ApplyFeatureSetResponse.Status.CREATED));
      assertThat(response.getFeatureSet().getSpec().getLabelsMap(), hasEntry("label", "some"));
    }
  }

  @Nested
  class ApplyEntity {
    @Test
    public void shouldThrowExceptionGivenEntityWithDash() {
      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  apiClient.simpleApplyEntity(
                      "default",
                      DataGenerator.createEntitySpecV2(
                          "dash-entity",
                          "Dash Entity description",
                          ValueProto.ValueType.Enum.STRING,
                          ImmutableMap.of("test_key", "test_value"))));

      assertThat(
          exc.getMessage(),
          equalTo(
              String.format(
                  "INTERNAL: invalid value for %s resource, %s: %s",
                  "entity",
                  "dash-entity",
                  "argument must only contain alphanumeric characters and underscores.")));
    }

    @Test
    public void shouldThrowExceptionIfTypeChanged() {
      String projectName = "default";

      EntityProto.EntitySpecV2 spec =
          DataGenerator.createEntitySpecV2(
              "entity1",
              "Entity description",
              ValueProto.ValueType.Enum.FLOAT,
              ImmutableMap.of("label_key", "label_value"));

      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class, () -> apiClient.simpleApplyEntity("default", spec));

      assertThat(
          exc.getMessage(),
          equalTo(
              String.format(
                  "INTERNAL: You are attempting to change the type of this entity in %s project from %s to %s. This isn't allowed. Please create a new entity.",
                  "default", "STRING", spec.getValueType())));
    }

    @Test
    public void shouldReturnEntityIfEntityHasNotChanged() {
      String projectName = "default";
      EntityProto.EntitySpecV2 spec = apiClient.simpleGetEntity(projectName, "entity1").getSpec();

      CoreServiceProto.ApplyEntityResponse response =
          apiClient.simpleApplyEntity(projectName, spec);

      assertThat(response.getEntity().getSpec().getName(), equalTo(spec.getName()));
      assertThat(response.getEntity().getSpec().getDescription(), equalTo(spec.getDescription()));
      assertThat(response.getEntity().getSpec().getLabelsMap(), equalTo(spec.getLabelsMap()));
      assertThat(response.getEntity().getSpec().getValueType(), equalTo(spec.getValueType()));
    }

    @Test
    public void shouldApplyEntityIfNotExists() {
      String projectName = "default";
      EntityProto.EntitySpecV2 spec =
          DataGenerator.createEntitySpecV2(
              "new_entity",
              "Entity description",
              ValueProto.ValueType.Enum.STRING,
              ImmutableMap.of("label_key", "label_value"));

      CoreServiceProto.ApplyEntityResponse response =
          apiClient.simpleApplyEntity(projectName, spec);

      assertThat(response.getEntity().getSpec().getName(), equalTo(spec.getName()));
      assertThat(response.getEntity().getSpec().getDescription(), equalTo(spec.getDescription()));
      assertThat(response.getEntity().getSpec().getLabelsMap(), equalTo(spec.getLabelsMap()));
      assertThat(response.getEntity().getSpec().getValueType(), equalTo(spec.getValueType()));
    }

    @Test
    public void shouldCreateProjectWhenNotAlreadyExists() {
      EntityProto.EntitySpecV2 spec =
          DataGenerator.createEntitySpecV2(
              "new_entity2",
              "Entity description",
              ValueProto.ValueType.Enum.STRING,
              ImmutableMap.of("key1", "val1"));
      CoreServiceProto.ApplyEntityResponse response =
          apiClient.simpleApplyEntity("new_project", spec);

      assertThat(response.getEntity().getSpec().getName(), equalTo(spec.getName()));
      assertThat(response.getEntity().getSpec().getDescription(), equalTo(spec.getDescription()));
      assertThat(response.getEntity().getSpec().getLabelsMap(), equalTo(spec.getLabelsMap()));
      assertThat(response.getEntity().getSpec().getValueType(), equalTo(spec.getValueType()));
    }

    @Test
    public void shouldFailWhenProjectIsArchived() {
      apiClient.createProject("archived");
      apiClient.archiveProject("archived");

      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  apiClient.simpleApplyEntity(
                      "archived",
                      DataGenerator.createEntitySpecV2(
                          "new_entity3",
                          "Entity description",
                          ValueProto.ValueType.Enum.STRING,
                          ImmutableMap.of("key1", "val1"))));
      assertThat(exc.getMessage(), equalTo("INTERNAL: Project is archived: archived"));
    }

    @Test
    public void shouldUpdateLabels() {
      EntityProto.EntitySpecV2 spec =
          DataGenerator.createEntitySpecV2(
              "entity1",
              "Entity description",
              ValueProto.ValueType.Enum.STRING,
              ImmutableMap.of("label_key", "label_value", "label_key2", "label_value2"));

      CoreServiceProto.ApplyEntityResponse response = apiClient.simpleApplyEntity("default", spec);

      assertThat(response.getEntity().getSpec().getLabelsMap(), equalTo(spec.getLabelsMap()));
    }
  }

  @Nested
  class UpdateStore {
    @Test
    public void shouldUpdateStoreIfConfigChanges() {
      StoreProto.Store defaultStore = DataGenerator.getDefaultStore();

      StoreProto.Store updatedStore =
          DataGenerator.createStore(
              defaultStore.getName(),
              defaultStore.getType(),
              ImmutableList.of(Triple.of("project1", "*", false)));

      CoreServiceProto.UpdateStoreResponse response = apiClient.updateStore(updatedStore);
      assertThat(
          response.getStatus(), equalTo(CoreServiceProto.UpdateStoreResponse.Status.UPDATED));
    }

    @Test
    public void shouldDoNothingIfNoChange() {
      CoreServiceProto.UpdateStoreResponse response =
          apiClient.updateStore(DataGenerator.getDefaultStore());
      assertThat(
          response.getStatus(), equalTo(CoreServiceProto.UpdateStoreResponse.Status.NO_CHANGE));
    }
  }

  @Nested
  class GetFeatureSet {
    @Test
    public void shouldThrowExceptionGivenMissingFeatureSet() {
      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class, () -> apiClient.getFeatureSet("default", "unknown"));

      assertThat(
          exc.getMessage(),
          equalTo("INTERNAL: Feature set with name \"unknown\" could not be found."));
    }
  }

  @Nested
  class GetEntity {
    @Test
    public void shouldThrowExceptionGivenMissingEntity() {
      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class, () -> apiClient.simpleGetEntity("default", ""));

      assertThat(exc.getMessage(), equalTo("INVALID_ARGUMENT: No entity name provided"));
    }

    public void shouldRetrieveFromDefaultIfProjectNotSpecified() {
      String entityName = "entity1";
      EntityProto.Entity entity = apiClient.simpleGetEntity("", entityName);

      assertThat(entity.getSpec().getName(), equalTo(entityName));
    }
  }

  @Nested
  class GetFeatureTable {
    @Test
    public void shouldThrowExceptionGivenNoSuchFeatureTable() {
      String projectName = "default";
      String featureTableName = "invalid_table";
      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class,
              () -> apiClient.simpleGetFeatureTable(projectName, featureTableName));

      assertThat(
          exc.getMessage(),
          equalTo(
              String.format(
                  "NOT_FOUND: No such Feature Table: (project: %s, name: %s)",
                  projectName, featureTableName)));
    }

    @Test
    public void shouldReturnFeatureTableIfExists() {
      FeatureTableSpec featureTableSpec =
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
                  DataGenerator.createFileDataSourceSpec(
                      "file:///path/to/file", "ts_col", ""))
              .build();
      FeatureTableProto.FeatureTable featureTable =
          apiClient.simpleGetFeatureTable("default", "featuretable1");

      assertTrue(TestUtil.compareFeatureTableSpec(featureTable.getSpec(), featureTableSpec));
    }
  }

  @Nested
  class ListStores {
    @Test
    public void shouldReturnAllStoresIfNoNameProvided() {
      apiClient.updateStore(DataGenerator.getDefaultStore());
      apiClient.updateStore(
          DataGenerator.createStore(
              "data", StoreProto.Store.StoreType.REDIS, Collections.emptyList()));

      List<StoreProto.Store> actual =
          stub.listStores(
                  CoreServiceProto.ListStoresRequest.newBuilder()
                      .setFilter(CoreServiceProto.ListStoresRequest.Filter.newBuilder().build())
                      .build())
              .getStoreList();

      assertThat(actual, hasSize(2));
      assertThat(actual, hasItem(hasProperty("name", equalTo("test-store"))));
      assertThat(actual, hasItem(hasProperty("name", equalTo("data"))));
    }

    @Test
    public void shouldThrowRetrievalExceptionIfNoStoresFoundWithName() {
      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  stub.listStores(
                      CoreServiceProto.ListStoresRequest.newBuilder()
                          .setFilter(
                              CoreServiceProto.ListStoresRequest.Filter.newBuilder()
                                  .setName("unknown")
                                  .build())
                          .build()));

      assertThat(exc.getMessage(), equalTo("INTERNAL: Store with name 'unknown' not found"));
    }
  }

  @Nested
  class ListFeatures {
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

  @Nested
  public class ApplyFeatureTable {
    private FeatureTableSpec getTestSpec() {
      return DataGenerator.createFeatureTableSpec(
              "ft",
              List.of("entity1", "entity2"),
              Map.of(
                  "feature1", ValueProto.ValueType.Enum.INT64,
                  "feature2", ValueProto.ValueType.Enum.FLOAT),
              3600,
              Map.of())
          .toBuilder()
          .setBatchSource(
              DataGenerator.createFileDataSourceSpec(
                  "file:///path/to/file", "ts_col", ""))
          .setStreamSource(
              DataGenerator.createKafkaDataSourceSpec(
                  "localhost:9092", "topic", "class.path", "ts_col"))
          .build();
    }

    @Test
    public void shouldApplyNewValidTable() {
      FeatureTableProto.FeatureTable table = apiClient.applyFeatureTable("default", getTestSpec());

      assertTrue(TestUtil.compareFeatureTableSpec(table.getSpec(), getTestSpec()));
      assertThat(table.getMeta().getRevision(), equalTo(0L));
    }

    @Test
    public void shouldUpdateExistingTableWithValidSpec() {
      FeatureTableProto.FeatureTable table = apiClient.applyFeatureTable("default", getTestSpec());

      FeatureTableSpec updatedSpec =
          DataGenerator.createFeatureTableSpec(
                  "ft",
                  List.of("entity1", "entity2"),
                  Map.of(
                      "feature2", ValueProto.ValueType.Enum.FLOAT,
                      "feature3", ValueProto.ValueType.Enum.INT64,
                      "feature4", ValueProto.ValueType.Enum.INT64),
                  2100,
                  Map.of("test", "labels"))
              .toBuilder()
              .setStreamSource(
                  DataGenerator.createFileDataSourceSpec(
                      "file:///path/to/file", "ts_col", ""))
              .setBatchSource(
                  DataGenerator.createKafkaDataSourceSpec(
                      "localhost:9092", "topic", "class.path", "ts_col"))
              .build();
      FeatureTableProto.FeatureTable updatedTable =
          apiClient.applyFeatureTable("default", updatedSpec);

      assertTrue(TestUtil.compareFeatureTableSpec(updatedTable.getSpec(), updatedSpec));
      assertThat(updatedTable.getMeta().getRevision(), equalTo(table.getMeta().getRevision() + 1L));
    }

    @Test
    public void shouldNotUpdateIfNoChanges() {
      FeatureTableProto.FeatureTable table = apiClient.applyFeatureTable("default", getTestSpec());
      FeatureTableProto.FeatureTable updatedTable =
          apiClient.applyFeatureTable("default", getTestSpec());

      assertThat(updatedTable.getMeta().getRevision(), equalTo(table.getMeta().getRevision()));
    }

    @Test
    public void shouldErrorOnMissingBatchSource() {
      FeatureTableProto.FeatureTableSpec spec =
          DataGenerator.createFeatureTableSpec(
                  "ft",
                  List.of("entity1"),
                  Map.of("event_timestamp", ValueProto.ValueType.Enum.INT64),
                  3600,
                  Map.of())
              .toBuilder()
              .build();

      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class, () -> apiClient.applyFeatureTable("default", spec));

      assertThat(
          exc.getMessage(),
          equalTo("INVALID_ARGUMENT: FeatureTable batch source cannot be empty."));
    }

    @Test
    public void shouldErrorIfEntityChangeOnUpdate() {
      List<String> entities = Arrays.asList("entity1", "entity2");
      FeatureTableProto.FeatureTableSpec spec =
          DataGenerator.createFeatureTableSpec(
                  "featuretable1",
                  Arrays.asList("entity1"),
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
                  DataGenerator.createFileDataSourceSpec(
                      "file:///path/to/file", "ts_col", ""))
              .build();

      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class, () -> apiClient.applyFeatureTable("default", spec));

      assertThat(
          exc.getMessage(),
          equalTo(
              String.format(
                  "INVALID_ARGUMENT: Updating the entities of a registered FeatureTable is not allowed: %s to %s",
                  entities, spec.getEntitiesList())));
    }

    @Test
    public void shouldErrorIfFeatureValueTypeChangeOnUpdate() {
      FeatureTableProto.FeatureTableSpec spec =
          DataGenerator.createFeatureTableSpec(
                  "featuretable1",
                  Arrays.asList("entity1", "entity2"),
                  new HashMap<>() {
                    {
                      put("feature1", ValueProto.ValueType.Enum.STRING);
                      put("feature2", ValueProto.ValueType.Enum.STRING_LIST);
                    }
                  },
                  7200,
                  ImmutableMap.of("feat_key2", "feat_value2"))
              .toBuilder()
              .setBatchSource(
                  DataGenerator.createFileDataSourceSpec(
                      "file:///path/to/file", "ts_col", ""))
              .build();

      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class, () -> apiClient.applyFeatureTable("default", spec));

      assertThat(
          exc.getMessage(),
          equalTo(
              String.format(
                  "INVALID_ARGUMENT: Updating the value type of a registered Feature is not allowed: %s to %s",
                  ValueProto.ValueType.Enum.FLOAT, ValueProto.ValueType.Enum.STRING_LIST)));
    }

    @Test
    public void shouldErrorOnInvalidBigQueryTableRef() {
      String invalidTableRef = "invalid.bq:path";
      FeatureTableProto.FeatureTableSpec spec =
          DataGenerator.createFeatureTableSpec(
                  "ft",
                  List.of("entity1"),
                  Map.of("feature", ValueProto.ValueType.Enum.INT64),
                  3600,
                  Map.of())
              .toBuilder()
              .setBatchSource(
                  DataGenerator.createBigQueryDataSourceSpec(invalidTableRef, "ts_col", ""))
              .build();

      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class, () -> apiClient.applyFeatureTable("default", spec));

      assertThat(
          exc.getMessage(),
          equalTo(
              String.format(
                  "INVALID_ARGUMENT: invalid value for FeatureTable resource, %s: argument must be in the form of <project:dataset.table> .",
                  invalidTableRef)));
    }

    @Test
    public void shouldErrorOnReservedNames() {
      // Reserved name used as feature name
      assertThrows(
          StatusRuntimeException.class,
          () ->
              apiClient.applyFeatureTable(
                  "default",
                  DataGenerator.createFeatureTableSpec(
                          "ft",
                          List.of("entity1"),
                          Map.of("event_timestamp", ValueProto.ValueType.Enum.INT64),
                          3600,
                          Map.of())
                      .toBuilder()
                      .setBatchSource(
                          DataGenerator.createFileDataSourceSpec(
                              "file:///path/to/file", "ts_col", ""))
                      .build()));

      // Reserved name used in as entity name
      assertThrows(
          StatusRuntimeException.class,
          () ->
              apiClient.applyFeatureTable(
                  "default",
                  DataGenerator.createFeatureTableSpec(
                          "ft",
                          List.of("created_timestamp"),
                          Map.of("feature1", ValueProto.ValueType.Enum.INT64),
                          3600,
                          Map.of())
                      .toBuilder()
                      .setBatchSource(
                          DataGenerator.createFileDataSourceSpec(
                              "file:///path/to/file", "ts_col", ""))
                      .build()));
    }

    @Test
    public void shouldErrorOnInvalidName() {
      // Invalid feature table name
      assertThrows(
          StatusRuntimeException.class,
          () ->
              apiClient.applyFeatureTable(
                  "default",
                  DataGenerator.createFeatureTableSpec(
                          "f-t",
                          List.of("entity1"),
                          Map.of("feature1", ValueProto.ValueType.Enum.INT64),
                          3600,
                          Map.of())
                      .toBuilder()
                      .setBatchSource(
                          DataGenerator.createFileDataSourceSpec(
                              "file:///path/to/file", "ts_col", ""))
                      .build()));

      // Invalid feature name
      assertThrows(
          StatusRuntimeException.class,
          () ->
              apiClient.applyFeatureTable(
                  "default",
                  DataGenerator.createFeatureTableSpec(
                          "ft",
                          List.of("entity1"),
                          Map.of("feature-1", ValueProto.ValueType.Enum.INT64),
                          3600,
                          Map.of())
                      .toBuilder()
                      .setBatchSource(
                          DataGenerator.createFileDataSourceSpec(
                              "file:///path/to/file", "ts_col", ""))
                      .build()));
    }

    @Test
    public void shouldErrorOnNotFoundEntityName() {
      assertThrows(
          StatusRuntimeException.class,
          () ->
              apiClient.applyFeatureTable(
                  "default",
                  DataGenerator.createFeatureTableSpec(
                          "ft1",
                          List.of("entity_not_found"),
                          Map.of("feature1", ValueProto.ValueType.Enum.INT64),
                          3600,
                          Map.of())
                      .toBuilder()
                      .setBatchSource(
                          DataGenerator.createFileDataSourceSpec(
                              "file:///path/to/file", "ts_col", ""))
                      .build()));
    }

    @Test
    public void shouldErrorOnArchivedProject() {
      apiClient.createProject("archived");
      apiClient.archiveProject("archived");

      assertThrows(
          StatusRuntimeException.class,
          () ->
              apiClient.applyFeatureTable(
                  "archived",
                  DataGenerator.createFeatureTableSpec(
                          "ft1",
                          List.of("entity1", "entity2"),
                          Map.of("feature1", ValueProto.ValueType.Enum.INT64),
                          3600,
                          Map.of())
                      .toBuilder()
                      .setBatchSource(
                          DataGenerator.createFileDataSourceSpec(
                              "file:///path/to/file", "ts_col", ""))
                      .build()));
    }
  }
}
