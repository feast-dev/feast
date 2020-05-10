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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.util.Timestamps;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import feast.common.models.FeatureSetReference;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.StoreProto;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FieldProto;
import feast.proto.types.ValueProto;
import feast.proto.types.ValueProto.ValueType.Enum;
import feast.storage.connectors.snowflake.common.DatabaseTemplater;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.jdbc.core.JdbcTemplate;

public class JdbcSnowflakeFeatureSinkMockTest {
  @Rule public transient TestPipeline p = TestPipeline.create();

  private SnowflakeFeatureSink snowflakeFeatureSinkObj;
  private StoreProto.Store.SnowflakeConfig snowflakeConfig;
  private String userName = "fakeUsername";
  private String password = "fakePassword";
  private String database = "DEMO_DB";
  private String schema = "PUBLIC";
  private String warehouse = "COMPUTE_WH";
  private String snowflakeUrl = "jdbc:snowflake://nx46274.us-east-2.aws.snowflakecomputing.com";
  private String className = "net.snowflake.client.jdbc.SnowflakeDriver";
  private String tableName = "feast_features";
  private Map<FeatureSetReference, FeatureSetProto.FeatureSetSpec> specMap;
  @Mock JdbcTemplate jdbcTemplate;
  private String testDriverClassName = "org.sqlite.JDBC";
  private String testDbUrl = "jdbc:sqlite:memory:myDb";
  private String testDbUsername = "sa";
  private String tesDbPassword = "sa";
  private List<FeatureRow> testFeatureRows;

  @Before
  public void setUp() throws ParseException {
    MockitoAnnotations.initMocks(this);
    // setup featuresets
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

    specMap = ImmutableMap.of(ref1, spec1, ref2, spec2);
    this.snowflakeConfig =
        StoreProto.Store.SnowflakeConfig.newBuilder()
            .setUrl(this.snowflakeUrl)
            .setClassName(this.className)
            .setUsername(this.userName)
            .setPassword(this.password)
            .setDatabase(this.database)
            .setSchema(this.schema)
            .setWarehouse(this.warehouse)
            .setTableName(this.tableName)
            .setBatchSize(1) // This must be set to 1 for DirectRunner
            .build();
    this.snowflakeFeatureSinkObj =
        (SnowflakeFeatureSink) SnowflakeFeatureSink.fromConfig(this.snowflakeConfig);
    String event_timestamp = "2019-12-31T16:00:00.00Z";
    testFeatureRows =
        ImmutableList.of(
            FeatureRow.newBuilder()
                .setFeatureSet("snowflake_proj/feature_set_1")
                .setEventTimestamp(Timestamps.parse(event_timestamp))
                .addFields(field("entity", 1, Enum.INT64))
                .build(),
            FeatureRow.newBuilder()
                .setFeatureSet("snowflake_proj/feature_set_2")
                .addFields(field("entity", 2, Enum.INT64))
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
                .addFields(field("entity_id_secondary", "asjdh", Enum.STRING))
                .build());
  }

  @Test
  public void shouldStartPrepareWrite() {
    // Mocking the jdbcTemplate
    SnowflakeFeatureSink fakeSnowflakeFeatureSinkObj = spy(snowflakeFeatureSinkObj);
    doNothing().when(jdbcTemplate).execute(any(String.class));
    Mockito.doReturn(jdbcTemplate).when(fakeSnowflakeFeatureSinkObj).createJdbcTemplate();
    PCollection<KV<FeatureSetReference, FeatureSetProto.FeatureSetSpec>> featureSetSpecs =
        p.apply("CreateSchema", Create.of(specMap));
    PCollection<FeatureSetReference> actualSchemas =
        fakeSnowflakeFeatureSinkObj.prepareWrite(featureSetSpecs);
    String actualName = actualSchemas.getName();
    p.run();
    Assert.assertEquals("createSchema/ParMultiDo(Anonymous).output", actualName);
  }

  @Test(expected = Pipeline.PipelineExecutionException.class)
  public void shouldNotWriteToSnowflakeTableNotExists() {
    JdbcIO.DataSourceConfiguration testConfig =
        JdbcIO.DataSourceConfiguration.create(this.testDriverClassName, this.testDbUrl)
            .withUsername(this.testDbUsername)
            .withPassword(this.tesDbPassword);

    // Mock databaseTemplater and jdbcWrite for testing on Sqlite
    DatabaseTemplater databaseTemplater = this.snowflakeFeatureSinkObj.getDatabaseTemplater();
    DatabaseTemplater testDatabaseTemplater = spy(databaseTemplater);
    SnowflakeWrite snowflakeWrite = new SnowflakeWrite(this.snowflakeConfig, testDatabaseTemplater);
    SnowflakeWrite testSnowflakeWrite = Mockito.spy(snowflakeWrite);
    when(testSnowflakeWrite.create_dsconfig(this.snowflakeConfig)).thenReturn(testConfig);
    // Drop the feature set table if exists
    JdbcTemplate jdbcTemplate = createTestJdbcTemplate();
    String dropTable = "DROP TABLE IF EXISTS feast_features";
    jdbcTemplate.execute(dropTable);
    String testInsertionSql =
        "INSERT INTO feast_features (event_timestamp,created_timestamp,project,featureset,feature,ingestion_id,job_id) select ?,?,?,?,json(?),?,?;";
    when(testDatabaseTemplater.getFeatureRowInsertSql(this.tableName)).thenReturn(testInsertionSql);

    // Create feast_features table
    p.apply(Create.of(testFeatureRows)).apply(testSnowflakeWrite);
    p.run();
  }

  @Test
  public void shouldWriteToSnowflake() {
    JdbcIO.DataSourceConfiguration testConfig =
        JdbcIO.DataSourceConfiguration.create(this.testDriverClassName, this.testDbUrl)
            .withUsername(this.testDbUsername)
            .withPassword(this.tesDbPassword);

    // Mock databaseTemplater and jdbcWrite for testing on Sqlite
    DatabaseTemplater databaseTemplater = this.snowflakeFeatureSinkObj.getDatabaseTemplater();
    DatabaseTemplater testDatabaseTemplater = spy(databaseTemplater);
    SnowflakeWrite snowflakeWrite = new SnowflakeWrite(this.snowflakeConfig, testDatabaseTemplater);
    SnowflakeWrite testSnowflakeWrite = Mockito.spy(snowflakeWrite);
    when(testSnowflakeWrite.create_dsconfig(this.snowflakeConfig)).thenReturn(testConfig);
    String testInsertionSql =
        "INSERT INTO feast_features (event_timestamp,created_timestamp,project,featureset,feature,ingestion_id,job_id) select ?,?,?,?,json(?),?,?;";
    when(testDatabaseTemplater.getFeatureRowInsertSql(this.tableName)).thenReturn(testInsertionSql);

    // Create feast_features table
    String createSqlTableCreationQuery =
        testDatabaseTemplater.getTableCreationSql(this.snowflakeConfig);
    JdbcTemplate jdbcTemplate = createTestJdbcTemplate();
    jdbcTemplate.execute(createSqlTableCreationQuery);
    p.apply(Create.of(testFeatureRows)).apply(testSnowflakeWrite);
    p.run();
    List<Map<String, Object>> resultsRows =
        jdbcTemplate.queryForList("SELECT * FROM feast_features WHERE featureset='feature_set_1'");
    String expectedProject = "snowflake_proj";
    Assert.assertTrue(resultsRows.size() > 0);
    Assert.assertEquals(expectedProject, resultsRows.get(0).get("project"));
  }

  private JdbcTemplate createTestJdbcTemplate() {
    HikariConfig hkConfig = new HikariConfig();
    hkConfig.setMaximumPoolSize(1);
    hkConfig.setDriverClassName(this.testDriverClassName);
    hkConfig.setJdbcUrl(this.testDbUrl);
    hkConfig.setUsername(this.testDbUsername);
    hkConfig.setPassword(this.tesDbPassword);
    final HikariDataSource ds = new HikariDataSource(hkConfig);
    return new JdbcTemplate(ds);
  }
}
