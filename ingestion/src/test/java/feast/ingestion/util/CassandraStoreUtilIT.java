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
package feast.ingestion.util;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import feast.core.StoreProto.Store.CassandraConfig;
import feast.ingestion.utils.StoreUtil;
import feast.store.serving.cassandra.CassandraMutation;
import feast.test.TestUtil.LocalCassandra;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CassandraStoreUtilIT {

  @BeforeClass
  public static void startServer() throws InterruptedException, IOException, TTransportException {
    LocalCassandra.start();
  }

  @After
  public void teardown() {
    LocalCassandra.stop();
  }

  @Test
  public void setupCassandra_shouldCreateKeyspaceAndTable() {
    CassandraConfig config =
        CassandraConfig.newBuilder()
            .setBootstrapHosts(LocalCassandra.getHost())
            .setPort(LocalCassandra.getPort())
            .setKeyspace("test")
            .setTableName("feature_store")
            .putAllReplicationOptions(
                new HashMap<String, String>() {
                  {
                    put("class", "NetworkTopologyStrategy");
                    put("dc1", "2");
                    put("dc2", "3");
                  }
                })
            .build();
    StoreUtil.setupCassandra(config);

    Map<String, String> actualReplication =
        LocalCassandra.getCluster().getMetadata().getKeyspace("test").getReplication();
    Map<String, String> expectedReplication =
        new HashMap<String, String>() {
          {
            put("class", "org.apache.cassandra.locator.NetworkTopologyStrategy");
            put("dc1", "2");
            put("dc2", "3");
          }
        };
    TableMetadata tableMetadata =
        LocalCassandra.getCluster().getMetadata().getKeyspace("test").getTable("feature_store");

    Assert.assertEquals(expectedReplication, actualReplication);
    Assert.assertNotNull(tableMetadata);
  }

  @Test
  public void setupCassandra_shouldBeIdempotent_whenTableAlreadyExistsAndSchemaMatches() {
    CassandraConfig config =
        CassandraConfig.newBuilder()
            .setBootstrapHosts(LocalCassandra.getHost())
            .setPort(LocalCassandra.getPort())
            .setKeyspace("test")
            .setTableName("feature_store")
            .putAllReplicationOptions(
                new HashMap<String, String>() {
                  {
                    put("class", "SimpleStrategy");
                    put("replication_factor", "2");
                  }
                })
            .build();

    LocalCassandra.createKeyspaceAndTable(config);

    // Check table is created
    Assert.assertNotNull(
        LocalCassandra.getCluster().getMetadata().getKeyspace("test").getTable("feature_store"));

    StoreUtil.setupCassandra(config);

    Assert.assertNotNull(
        LocalCassandra.getCluster().getMetadata().getKeyspace("test").getTable("feature_store"));
  }

  @Test(expected = RuntimeException.class)
  public void setupCassandra_shouldThrowException_whenTableNameDoesNotMatchObjectMapper() {
    CassandraConfig config =
        CassandraConfig.newBuilder()
            .setBootstrapHosts(LocalCassandra.getHost())
            .setPort(LocalCassandra.getPort())
            .setKeyspace("test")
            .setTableName("test_data_store")
            .putAllReplicationOptions(
                new HashMap<String, String>() {
                  {
                    put("class", "NetworkTopologyStrategy");
                    put("dc1", "2");
                    put("dc2", "3");
                  }
                })
            .build();
    StoreUtil.setupCassandra(config);
  }

  @Test(expected = RuntimeException.class)
  public void setupCassandra_shouldThrowException_whenTableSchemaDoesNotMatchObjectMapper() {
    LocalCassandra.getSession()
        .execute(
            "CREATE KEYSPACE test "
                + "WITH REPLICATION = {"
                + "'class': 'SimpleStrategy', 'replication_factor': 2 }");

    Create createTable =
        SchemaBuilder.createTable("test", "feature_store")
            .ifNotExists()
            .addPartitionKey(CassandraMutation.ENTITIES, DataType.text())
            .addClusteringColumn(
                "featureName", DataType.text()) // Column name does not match in CassandraMutation
            .addColumn(CassandraMutation.VALUE, DataType.blob());
    LocalCassandra.getSession().execute(createTable);

    CassandraConfig config =
        CassandraConfig.newBuilder()
            .setBootstrapHosts(LocalCassandra.getHost())
            .setPort(LocalCassandra.getPort())
            .setKeyspace("test")
            .setTableName("feature_store")
            .putAllReplicationOptions(
                new HashMap<String, String>() {
                  {
                    put("class", "SimpleStrategy");
                    put("replication_factor", "2");
                  }
                })
            .build();

    StoreUtil.setupCassandra(config);
  }
}
