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
package feast.ingestion.transform;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.FeatureSetProto.FeatureSet;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.CassandraConfig;
import feast.core.StoreProto.Store.StoreType;
import feast.test.TestUtil;
import feast.test.TestUtil.LocalCassandra;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto.Field;
import feast.types.ValueProto.Value;
import feast.types.ValueProto.ValueType.Enum;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

public class CassandraWriteToStoreIT implements Serializable {
  private FeatureSetSpec featureSetSpec;
  private FeatureRow row;

  class FakeCassandraWriteToStore extends WriteToStore {

    private FeatureSetSpec featureSetSpec;

    FakeCassandraWriteToStore(FeatureSetSpec featureSetSpec) {
      this.featureSetSpec = featureSetSpec;
    }

    @Override
    public Store getStore() {
      return Store.newBuilder()
          .setType(StoreType.CASSANDRA)
          .setName("SERVING")
          .setCassandraConfig(getCassandraConfig())
          .build();
    }

    @Override
    public Map<String, FeatureSet> getFeatureSets() {
      return new HashMap<String, FeatureSet>() {
        {
          put(featureSetSpec.getName() + ":" + featureSetSpec.getVersion(), FeatureSet.newBuilder().setSpec(featureSetSpec).build());
        }
      };
    }
  }

  private static CassandraConfig getCassandraConfig() {
    return CassandraConfig.newBuilder()
        .setBootstrapHosts(LocalCassandra.getHost())
        .setPort(LocalCassandra.getPort())
        .setTableName("feature_store")
        .setKeyspace("test")
        .putAllReplicationOptions(
            new HashMap<String, String>() {
              {
                put("class", "SimpleStrategy");
                put("replication_factor", "1");
              }
            })
        .build();
  }

  @BeforeClass
  public static void startServer() throws InterruptedException, IOException, TTransportException {
    LocalCassandra.start();
    LocalCassandra.createKeyspaceAndTable(getCassandraConfig());
  }

  @Before
  public void setUp() {
    featureSetSpec =
        TestUtil.createFeatureSetSpec(
            "fs",
            1,
            10,
            new HashMap<String, Enum>() {
              {
                put("entity1", Enum.INT64);
                put("entity2", Enum.STRING);
              }
            },
            new HashMap<String, Enum>() {
              {
                put("feature1", Enum.INT64);
                put("feature2", Enum.INT64);
              }
            });
    row =
        TestUtil.createFeatureRow(
            featureSetSpec,
            100,
            new HashMap<String, Value>() {
              {
                put("entity1", TestUtil.intValue(1));
                put("entity2", TestUtil.strValue("a"));
                put("feature1", TestUtil.intValue(1));
                put("feature2", TestUtil.intValue(2));
              }
            });
  }

  @Rule public transient TestPipeline testPipeline = TestPipeline.create();

  @AfterClass
  public static void cleanUp() {
    LocalCassandra.stop();
  }

  @Test
  public void testWriteCassandra_happyPath() throws InvalidProtocolBufferException {
    PCollection<FeatureRow> input = testPipeline.apply(Create.of(row));

    input.apply(new FakeCassandraWriteToStore(featureSetSpec));

    testPipeline.run();

    ResultSet resultSet = LocalCassandra.getSession().execute("SELECT * FROM test.feature_store");
    List<Field> actualResults = getResults(resultSet);

    List<Field> expectedFields =
        Arrays.asList(
            Field.newBuilder().setName("feature1").setValue(TestUtil.intValue(1)).build(),
            Field.newBuilder().setName("feature2").setValue(TestUtil.intValue(2)).build());

    assertTrue(actualResults.containsAll(expectedFields));
    assertEquals(expectedFields.size(), actualResults.size());
  }

  @Test(timeout = 30000)
  public void testWriteCassandra_shouldNotRetrieveExpiredValues()
      throws InvalidProtocolBufferException {
    // Set max age to 1 second
    FeatureSetSpec featureSetSpec =
        TestUtil.createFeatureSetSpec(
            "fs",
            1,
            1,
            new HashMap<String, Enum>() {
              {
                put("entity1", Enum.INT64);
                put("entity2", Enum.STRING);
              }
            },
            new HashMap<String, Enum>() {
              {
                put("feature1", Enum.INT64);
                put("feature2", Enum.INT64);
              }
            });

    PCollection<FeatureRow> input = testPipeline.apply(Create.of(row));

    input.apply(new FakeCassandraWriteToStore(featureSetSpec));

    testPipeline.run();

    while (true) {
      ResultSet resultSet =
          LocalCassandra.getSession()
              .execute("SELECT feature, value, ttl(value) as expiry FROM test.feature_store");
      List<Field> results = getResults(resultSet);
      if (results.isEmpty()) break;
    }
  }

  @Test
  public void testWriteCassandra_shouldNotOverrideNewerValues()
      throws InvalidProtocolBufferException {
    FeatureRow olderRow =
        TestUtil.createFeatureRow(
            featureSetSpec,
            10,
            new HashMap<String, Value>() {
              {
                put("entity1", TestUtil.intValue(1));
                put("entity2", TestUtil.strValue("a"));
                put("feature1", TestUtil.intValue(3));
                put("feature2", TestUtil.intValue(4));
              }
            });

    PCollection<FeatureRow> input = testPipeline.apply(Create.of(row, olderRow));

    input.apply(new FakeCassandraWriteToStore(featureSetSpec));

    testPipeline.run();

    ResultSet resultSet = LocalCassandra.getSession().execute("SELECT * FROM test.feature_store");
    List<Field> actualResults = getResults(resultSet);

    List<Field> expectedFields =
        Arrays.asList(
            Field.newBuilder().setName("feature1").setValue(TestUtil.intValue(1)).build(),
            Field.newBuilder().setName("feature2").setValue(TestUtil.intValue(2)).build());

    assertTrue(actualResults.containsAll(expectedFields));
    assertEquals(expectedFields.size(), actualResults.size());
  }

  private List<Field> getResults(ResultSet resultSet) throws InvalidProtocolBufferException {
    List<Field> results = new ArrayList<>();
    while (!resultSet.isExhausted()) {
      Row row = resultSet.one();
      results.add(
          Field.newBuilder()
              .setName(row.getString("feature"))
              .setValue(Value.parseFrom(row.getBytes("value")))
              .build());
    }
    return results;
  }
}
