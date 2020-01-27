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
package feast.serving.service;

import static feast.serving.test.TestUtil.intValue;
import static feast.serving.test.TestUtil.responseToMapList;
import static feast.serving.test.TestUtil.strValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.utils.Bytes;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.serving.ServingAPIProto;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest.EntityRow;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse.FieldValues;
import feast.serving.specs.CachedSpecService;
import feast.serving.specs.FeatureSetRequest;
import feast.serving.test.TestUtil.LocalCassandra;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.ValueProto;
import feast.types.ValueProto.Value;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;

public class CassandraServingServiceITTest {

  @Mock CachedSpecService specService;

  @Mock Tracer tracer;

  private CassandraServingService cassandraServingService;
  private Session session;

  @BeforeClass
  public static void startServer() throws InterruptedException, IOException, TTransportException {
    LocalCassandra.start();
    LocalCassandra.createKeyspaceAndTable();
  }

  @Before
  public void setup() {
    initMocks(this);
    FeatureSetSpec featureSetSpec =
        FeatureSetSpec.newBuilder()
            .setProject("test_project")
            .setName("featureSet")
            .setVersion(1)
            .addEntities(EntitySpec.newBuilder().setName("entity1"))
            .addEntities(EntitySpec.newBuilder().setName("entity2"))
            .build();
    List<FeatureSetRequest> req = new ArrayList<FeatureSetRequest>();
    req.add(FeatureSetRequest.newBuilder().setSpec(featureSetSpec).build());
    List<ServingAPIProto.FeatureReference> ref = new ArrayList<ServingAPIProto.FeatureReference>();
    ref.add(
        ServingAPIProto.FeatureReference.newBuilder()
            .setName("featureSet")
            .setVersion(1)
            .setProject("test_project")
            .build());
    System.out.printf("FS request return %s", specService.getFeatureSets(ref));
    when(specService.getFeatureSets(ref)).thenReturn(req);
    when(tracer.buildSpan(ArgumentMatchers.any())).thenReturn(Mockito.mock(SpanBuilder.class));

    session =
        new Cluster.Builder()
            .addContactPoints(LocalCassandra.getHost())
            .withPort(LocalCassandra.getPort())
            .build()
            .connect();

    populateTable(session);

    cassandraServingService =
        new CassandraServingService(session, "test", "feature_store", specService, tracer);
  }

  private void populateTable(Session session) {
    session.execute(
        insertQuery(
            "test",
            "feature_store",
            "test_project/featureSet:1:entity1=1|entity2=a",
            "feature1",
            intValue(1)));
    session.execute(
        insertQuery(
            "test",
            "feature_store",
            "test_project/featureSet:1:entity1=1|entity2=a",
            "feature2",
            intValue(1)));
    session.execute(
        insertQuery(
            "test",
            "feature_store",
            "test_project/featureSet:1:entity1=2|entity2=b",
            "feature1",
            intValue(1)));
    session.execute(
        insertQuery(
            "test",
            "feature_store",
            "test_project/featureSet:1:entity1=2|entity2=b",
            "feature2",
            intValue(1)));
  }

  @AfterClass
  public static void cleanUp() {
    LocalCassandra.stop();
  }

  @Test
  public void shouldReturnResponseWithValuesIfKeysPresent() {
    GetOnlineFeaturesRequest request =
        GetOnlineFeaturesRequest.newBuilder()
            .addAllFeatures(
                FeatureSetRequest.newBuilder()
                    .setSpec(
                        FeatureSetSpec.newBuilder()
                            .setName("featureSet")
                            .setProject("test_project")
                            .setVersion(1)
                            .addAllFeatures(
                                Lists.newArrayList(
                                    FeatureSpec.newBuilder()
                                        .setName("feature1")
                                        .setValueType(ValueProto.ValueType.Enum.INT64)
                                        .build(),
                                    FeatureSpec.newBuilder()
                                        .setName("feature2")
                                        .setValueType(ValueProto.ValueType.Enum.STRING)
                                        .build()))
                            .build())
                    .build()
                    .getFeatureReferences())
            .addEntityRows(
                EntityRow.newBuilder()
                    .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100))
                    .putFields("entity1", intValue(1))
                    .putFields("entity2", strValue("a")))
            .addEntityRows(
                EntityRow.newBuilder()
                    .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100))
                    .putFields("entity1", intValue(2))
                    .putFields("entity2", strValue("b")))
            .build();

    GetOnlineFeaturesResponse expected =
        GetOnlineFeaturesResponse.newBuilder()
            .addFieldValues(
                FieldValues.newBuilder()
                    .putFields("entity1", intValue(1))
                    .putFields("entity2", strValue("a"))
                    .putFields("fs/featureSet:1:feature1", intValue(1))
                    .putFields("fs/featureSet:1:feature2", intValue(1)))
            .addFieldValues(
                FieldValues.newBuilder()
                    .putFields("entity1", intValue(2))
                    .putFields("entity2", strValue("b"))
                    .putFields("test_project/featureSet:1:feature1", intValue(1))
                    .putFields("test_project/featureSet:1:feature2", intValue(1)))
            .build();

    GetOnlineFeaturesResponse actual = cassandraServingService.getOnlineFeatures(request);
    System.out.printf("ACTUAL %s\n", responseToMapList(actual));
    System.out.printf("EXPECTED %s\n", responseToMapList(expected));
    assertThat(
        responseToMapList(actual), containsInAnyOrder(responseToMapList(expected).toArray()));
  }

  @Test
  public void shouldReturnResponseWithUnsetValuesIfKeysNotPresent() {
    GetOnlineFeaturesRequest request =
        GetOnlineFeaturesRequest.newBuilder()
            .addAllFeatures(
                FeatureSetRequest.newBuilder()
                    .setSpec(
                        FeatureSetSpec.newBuilder()
                            .setName("featureSet")
                            .setProject("test_project")
                            .setVersion(1)
                            .addAllFeatures(
                                Lists.newArrayList(
                                    FeatureSpec.newBuilder()
                                        .setName("feature1")
                                        .setValueType(ValueProto.ValueType.Enum.INT64)
                                        .build(),
                                    FeatureSpec.newBuilder()
                                        .setName("feature2")
                                        .setValueType(ValueProto.ValueType.Enum.STRING)
                                        .build()))
                            .build())
                    .build()
                    .getFeatureReferences())
            .addEntityRows(
                EntityRow.newBuilder()
                    .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100))
                    .putFields("entity1", intValue(1))
                    .putFields("entity2", strValue("a")))
            // Non-existing entity keys
            .addEntityRows(
                EntityRow.newBuilder()
                    .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100))
                    .putFields("entity1", intValue(55))
                    .putFields("entity2", strValue("ff")))
            .build();

    GetOnlineFeaturesResponse expected =
        GetOnlineFeaturesResponse.newBuilder()
            .addFieldValues(
                FieldValues.newBuilder()
                    .putFields("entity1", intValue(1))
                    .putFields("entity2", strValue("a"))
                    .putFields("featureSet:1:feature1", intValue(1))
                    .putFields("featureSet:1:feature2", intValue(1)))
            // Missing keys will return empty values
            .addFieldValues(
                FieldValues.newBuilder()
                    .putFields("entity1", intValue(55))
                    .putFields("entity2", strValue("ff"))
                    .putFields("featureSet:1:feature1", Value.newBuilder().build())
                    .putFields("featureSet:1:feature2", Value.newBuilder().build()))
            .build();
    GetOnlineFeaturesResponse actual = cassandraServingService.getOnlineFeatures(request);

    assertThat(
        responseToMapList(actual), containsInAnyOrder(responseToMapList(expected).toArray()));
  }

  // This test should fail if cassandra no longer stores write time as microseconds or if we change
  // the way we parse microseconds to com.google.protobuf.Timestamp
  @Test
  public void shouldInsertAndParseWriteTimestampInMicroSeconds()
      throws InvalidProtocolBufferException {
    session.execute(
        "INSERT INTO test.feature_store (entities, feature, value)\n"
            + "  VALUES ('ENT1', 'FEAT1',"
            + Bytes.toHexString(Value.newBuilder().build().toByteArray())
            + ")\n"
            + "  USING TIMESTAMP 1574318287123456;");

    ResultSet resultSet =
        session.execute(
            QueryBuilder.select()
                .column("entities")
                .column("feature")
                .column("value")
                .writeTime("value")
                .as("writetime")
                .from("test", "feature_store")
                .where(QueryBuilder.eq("entities", "ENT1")));
    FeatureRow featureRow = cassandraServingService.parseResponse(resultSet);

    Assert.assertEquals(
        Timestamp.newBuilder().setSeconds(1574318287).setNanos(123456000).build(),
        featureRow.getEventTimestamp());
  }

  private Insert insertQuery(
      String database, String table, String key, String featureName, Value value) {
    return QueryBuilder.insertInto(database, table)
        .value("entities", key)
        .value("feature", featureName)
        .value("value", ByteBuffer.wrap(value.toByteArray()));
  }
}
