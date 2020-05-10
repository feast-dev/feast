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
package feast.storage.connectors.snowflake.writer;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import feast.common.models.FeatureSetReference;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.StoreProto;
import feast.proto.core.StoreProto.Store.SnowflakeConfig;
import feast.storage.api.writer.FeatureSink;
import feast.storage.connectors.snowflake.common.DatabaseTemplater;
import feast.storage.connectors.snowflake.snowflake.SnowflakeTemplater;
import java.util.Map;
import java.util.Properties;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;

public class SnowflakeFeatureSink implements FeatureSink {
  /** */
  private static final long serialVersionUID = 1L;

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(SnowflakeFeatureSink.class);

  private final StoreProto.Store.SnowflakeConfig config;
  //  private JdbcTemplate jdbcTemplate;
  public DatabaseTemplater getDatabaseTemplater() {
    return databaseTemplater;
  }

  private DatabaseTemplater databaseTemplater;

  public SnowflakeFeatureSink(SnowflakeConfig config) {
    this.config = config;
    this.databaseTemplater = getSnowflakeTemplaterForClass(config.getClassName());
  }

  private DatabaseTemplater getSnowflakeTemplaterForClass(String className) {
    switch (className) {
      case "net.snowflake.client.jdbc.SnowflakeDriver":
        return new SnowflakeTemplater();
      default:
        throw new RuntimeException(
            "JDBC class name was not specified, was incorrect, or had no implementation for templating.");
    }
  }

  public static FeatureSink fromConfig(SnowflakeConfig config) {
    return new SnowflakeFeatureSink(config);
  }

  public SnowflakeConfig getConfig() {
    return config;
  }

  @Override
  public PCollection<FeatureSetReference> prepareWrite(
      PCollection<KV<FeatureSetReference, FeatureSetProto.FeatureSetSpec>> featureSetSpecs) {
    PCollection<FeatureSetReference> schemas =
        featureSetSpecs.apply(
            "createSchema",
            ParDo.of(
                new DoFn<
                    KV<FeatureSetReference, FeatureSetProto.FeatureSetSpec>,
                    FeatureSetReference>() {

                  private static final long serialVersionUID = 1L;

                  @ProcessElement
                  public void processElement(
                      @Element KV<FeatureSetReference, FeatureSetProto.FeatureSetSpec> element,
                      OutputReceiver<FeatureSetReference> out,
                      ProcessContext context) {
                    out.output(element.getKey());
                  }
                }));

    Map<String, String> requiredColumns = this.databaseTemplater.getRequiredColumns();

    // create featureset table using jdbcTemplate
    String createSqlTableCreationQuery = this.databaseTemplater.getTableCreationSql(this.config);
    JdbcTemplate jdbcTemplate = createJdbcTemplate();
    jdbcTemplate.execute(createSqlTableCreationQuery);

    return schemas;
  }

  public JdbcTemplate createJdbcTemplate() {
    Properties dsProperties = new Properties();
    dsProperties.put("user", config.getUsername());
    dsProperties.put("password", config.getPassword());
    dsProperties.put("db", config.getDatabase());
    dsProperties.put("schema", config.getSchema());
    dsProperties.put("role", config.getRole());
    dsProperties.put("warehouse", config.getWarehouse());
    HikariConfig hkConfig = new HikariConfig();
    hkConfig.setMaximumPoolSize(100);
    hkConfig.setDriverClassName(config.getClassName());
    hkConfig.setDataSourceProperties(dsProperties);
    hkConfig.setJdbcUrl(config.getUrl());
    final HikariDataSource ds = new HikariDataSource(hkConfig);
    return new JdbcTemplate(ds);
  }

  @Override
  public SnowflakeWrite writer() {
    return new SnowflakeWrite(this.getConfig(), this.getDatabaseTemplater());
  }
}
