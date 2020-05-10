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
package feast.storage.connectors.snowflake.writer;

import static feast.storage.common.testing.TestUtil.field;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.util.Timestamps;
import feast.common.models.FeatureSetReference;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.StoreProto;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FieldProto;
import feast.proto.types.ValueProto;
import feast.proto.types.ValueProto.ValueType.Enum;
import feast.storage.api.writer.FeatureSink;
import java.sql.*;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.*;

@Ignore
public class JdbcSnowflakeFeatureSinkTest {
  /** Manual test needs a testing Snowflake account */
  @Rule public transient TestPipeline p = TestPipeline.create();

  private FeatureSink snowflakeFeatureSinkObj;

  // TODO: Update the variables to match your snowflake account

  private String userName = System.getenv("SNOWFLAKE_USERNAME");
  private String password = System.getenv("SNOWFLAKE_PASSWORD");

  private String database = "DEMO_DB";
  private String schema = "PUBLIC";
  private String warehouse = "COMPUTE_WH";
  private String snowflakeUrl = System.getenv("SNOWFLAKE_URL");
  private String className = "net.snowflake.client.jdbc.SnowflakeDriver";
  private String tableName = "feast_features";
  private String role = "ACCOUNTADMIN";
  private Connection conn;

  @Before
  public void setUp() {

    FeatureSetProto.FeatureSetSpec spec1 =
        FeatureSetProto.FeatureSetSpec.newBuilder()
            .setName("feature_set_1")
            .setProject("snowflake_proj")
            .build();

    FeatureSetReference ref1 = FeatureSetReference.of(spec1.getProject(), spec1.getName(), 1);

    FeatureSetProto.FeatureSetSpec spec2 =
        FeatureSetProto.FeatureSetSpec.newBuilder()
            .setName("feature_set_2")
            .setProject("snowflake_proj")
            .build();
    FeatureSetReference ref2 = FeatureSetReference.of(spec2.getProject(), spec2.getName(), 1);

    Map<FeatureSetReference, FeatureSetProto.FeatureSetSpec> specMap =
        ImmutableMap.of(ref1, spec1, ref2, spec2);
    this.snowflakeFeatureSinkObj =
        SnowflakeFeatureSink.fromConfig(
            StoreProto.Store.SnowflakeConfig.newBuilder()
                .setUrl(this.snowflakeUrl)
                .setClassName(this.className)
                .setUsername(this.userName)
                .setPassword(this.password)
                .setDatabase(this.database)
                .setSchema(this.schema)
                .setWarehouse(this.warehouse)
                .setTableName(this.tableName)
                .setRole(this.role)
                .setBatchSize(1) // This must be set to 1 for DirectRunner
                .build());

    this.snowflakeFeatureSinkObj.prepareWrite(p.apply("CreateSchema", Create.of(specMap)));

    this.connect();
  }

  private void connect() {
    if (this.conn != null) {
      return;
    }
    try {

      Class.forName(this.className);
      Properties props = new Properties();
      props.put("user", this.userName);
      props.put("password", this.password);
      props.put("db", this.database);
      props.put("schema", this.schema);
      props.put("role", this.role);
      DriverManager.getConnection(this.snowflakeUrl, props);
      this.conn = DriverManager.getConnection(this.snowflakeUrl, props);
    } catch (ClassNotFoundException | SQLException e) {
      System.err.println(e.getClass().getName() + ": " + e.getMessage());
    }
  }

  @Test
  public void shouldthrow() throws RuntimeException {

    String tableName = "fake_tableName";
    FeatureSink snowflakeFeatureSinkObj2 =
        SnowflakeFeatureSink.fromConfig(
            StoreProto.Store.SnowflakeConfig.newBuilder()
                .setUrl(this.snowflakeUrl)
                .setClassName(this.className)
                .setUsername(this.userName)
                .setPassword(this.password)
                .setDatabase(this.database)
                .setSchema(this.schema)
                .setWarehouse(this.warehouse)
                .setTableName(tableName)
                .setRole(this.role)
                .setBatchSize(1) // This must be set to 1 for DirectRunner
                .build());
    List<FeatureRow> featureRows =
        ImmutableList.of(
            FeatureRow.newBuilder()
                .setFeatureSet("snowflake_proj/feature_set_1")
                .addFields(field("entity", 1, Enum.INT64))
                .addFields(field("feature", "two", Enum.STRING))
                .build());
    try {
      p.apply(Create.of(featureRows)).apply(snowflakeFeatureSinkObj2.writer());

      p.run();
    } catch (RuntimeException re) {
      String message =
          String.format("Table '%s' does not exist or not authorized.", tableName.toUpperCase());
      Assert.assertTrue(re.getMessage().contains(message));
    }
  }

  @Test
  public void shouldWriteToSnowflake() throws SQLException, ParseException {

    String event_timestamp = "2019-12-31T16:00:00.00Z";
    List<FeatureRow> featureRows =
        ImmutableList.of(
            FeatureRow.newBuilder()
                .setFeatureSet("snowflake_proj/feature_set_1")
                .setEventTimestamp(Timestamps.parse(event_timestamp))
                .addFields(field("entity", 1, Enum.INT64))
                .addFields(field("feature", "two", Enum.STRING))
                .build(),
            FeatureRow.newBuilder()
                .setFeatureSet("snowflake_proj/feature_set_2")
                .addFields(field("entity", 2, Enum.INT64))
                .addFields(field("feature", "two", Enum.STRING))
                .addFields(field("entity_id_secondary", "asjdh", Enum.STRING))
                .build(),
            FeatureRow.newBuilder()
                .setFeatureSet("snowflake_proj/feature_set_2")
                .setIngestionId("table-4")
                .addFields(field("entity_id_primary", 4, Enum.INT32))
                .addFields(field("entity_id_secondary", "asjdh", Enum.STRING))
                .addFields(
                    FieldProto.Field.newBuilder()
                        .setName("feature_1")
                        .setValue(
                            ValueProto.Value.newBuilder()
                                .setStringListVal(
                                    ValueProto.StringList.newBuilder()
                                        .addVal("abc")
                                        .addVal("def")
                                        .build())
                                .build())
                        .build())
                .addFields(field("feature_2", 4, Enum.INT64))
                .build(),
            FeatureRow.newBuilder()
                .setFeatureSet("snowflake_proj/feature_set_3")
                .addFields(field("entity", 2, Enum.INT64))
                .addFields(field("feature", "two", Enum.STRING))
                .addFields(field("entity_id_secondary", "asjdh", Enum.STRING))
                .build());

    p.apply(Create.of(featureRows)).apply(this.snowflakeFeatureSinkObj.writer());

    p.run();

    DatabaseMetaData meta = conn.getMetaData();
    Assert.assertEquals(
        true, meta.getTables(null, null, this.tableName.toUpperCase(), null).next());
  }
}
