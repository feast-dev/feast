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
package feast.storage.connectors.snowflake.retriever;

import com.mitchellbosecke.pebble.PebbleEngine;
import com.mitchellbosecke.pebble.template.PebbleTemplate;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;

public class SnowflakeQueryTemplater extends AbstractJdbcQueryTemplater {
  private static final PebbleEngine engine = new PebbleEngine.Builder().build();
  protected static final String IMPORT_ENTITY_FILE_FORMAT =
      "(type='CSV' field_delimiter=',' skip_header=1)";
  protected static final String EXPORT_STAGING_FILE_FORMAT = "(type=csv compression='gzip')";
  private static final String FEATURESET_TEMPLATE_NAME_SNOWFLAKE =
      "templates/single_featureset_pit_join_snowflake.sql";
  private static final String JOIN_TEMPLATE_NAME_SNOWFLAKE =
      "templates/join_featuresets_snowflake.sql";
  private static final String VARIANT_COLUMN_NAME = "feature";
  private String storageIntegration;
  private String feastTable;

  public SnowflakeQueryTemplater(Map<String, String> databaseConfig, JdbcTemplate jdbcTemplate) {
    super(databaseConfig, jdbcTemplate);
    this.storageIntegration = databaseConfig.get("storage_integration");
    this.feastTable = databaseConfig.get("table_name");
  }

  @Override
  protected List<String> createEntityTableRowCountQuery(
      String destinationTable, List<FeatureSetQueryInfo> featureSetQueryInfos) {
    StringJoiner featureSetTableSelectJoiner = new StringJoiner(", ");
    StringJoiner featureSetTableFromJoiner = new StringJoiner(" CROSS JOIN ");
    Set<String> entities = new HashSet<>();
    List<String> entityColumns = new ArrayList<>();
    int count = 0;
    for (FeatureSetQueryInfo featureSetQueryInfo : featureSetQueryInfos) {
      String table = this.feastTable;
      String alias = String.format("a%s", count++);
      for (String entity : featureSetQueryInfo.getEntities()) {
        if (!entities.contains(entity)) {
          entities.add(entity);
          // parse entities from FEATURE variant column
          entityColumns.add(
              String.format("%s.%s:%s AS %s", alias, VARIANT_COLUMN_NAME, entity, entity));
        }
      }
      featureSetTableFromJoiner.add(String.format("%s AS %s", table, alias));
    }
    // Must preserve alphabetical order because column mapping isn't supported in COPY loads of CSV
    entityColumns.sort(Comparator.comparing(entity -> entity.split("\\.")[0]));
    entityColumns.forEach(featureSetTableSelectJoiner::add);

    List<String> createEntityTableRowCountQueries = new ArrayList<>();
    createEntityTableRowCountQueries.add(
        String.format(
            "CREATE TEMPORARY TABLE %s AS (SELECT %s FROM %s WHERE 1 = 2);",
            destinationTable, featureSetTableSelectJoiner, featureSetTableFromJoiner));
    createEntityTableRowCountQueries.add(
        String.format("ALTER TABLE %s ADD COLUMN event_timestamp TIMESTAMP;", destinationTable));
    return createEntityTableRowCountQueries;
  }

  @Override
  protected List<String> createLoadEntityQuery(String destinationTable, String entitySourceUri) {
    List<String> queries = new ArrayList<>();
    String copyIntoDestTable =
        String.format(
            "COPY INTO %s FROM '%s' FILE_FORMAT = %s on_error = 'skip_file' storage_integration = %s;",
            destinationTable, entitySourceUri, IMPORT_ENTITY_FILE_FORMAT, this.storageIntegration);
    String addRowNum =
        String.format(
            "CREATE OR REPLACE TEMPORARY TABLE %s as (SELECT *, ROW_NUMBER() OVER (ORDER BY 1) AS row_number FROM %s);",
            destinationTable, destinationTable);
    String[] queryArray = new String[] {copyIntoDestTable, addRowNum};
    queries.addAll(Arrays.asList(queryArray));
    //    System.out.println(String.format("Entity Table: %s", destinationTable));
    return queries;
  }

  @Override
  protected String createFeatureSetPointInTimeQuery(
      FeatureSetQueryInfo featureSetInfo,
      String entityTable,
      String minTimestamp,
      String maxTimestamp)
      throws IOException {
    PebbleTemplate template;
    template = engine.getTemplate(FEATURESET_TEMPLATE_NAME_SNOWFLAKE);

    Map<String, Object> context = new HashMap<>();
    context.put("variantColumn", VARIANT_COLUMN_NAME);
    context.put("featureSet", featureSetInfo);
    context.put("minTimestamp", minTimestamp);
    context.put("maxTimestamp", maxTimestamp);
    context.put("leftTableName", entityTable);
    context.put("feastTable", this.feastTable);

    Writer writer = new StringWriter();
    template.evaluate(writer, context);
    return writer.toString();
  }

  @Override
  protected String createJoinQuery(
      List<FeatureSetQueryInfo> featureSetInfos,
      List<String> entityTableColumnNames,
      String leftTableName) {

    PebbleTemplate template;
    template = engine.getTemplate(JOIN_TEMPLATE_NAME_SNOWFLAKE);
    Map<String, Object> context = new HashMap<>();
    context.put("entities", entityTableColumnNames);
    context.put("featureSets", featureSetInfos);
    context.put("leftTableName", leftTableName);

    Writer writer = new StringWriter();
    try {
      template.evaluate(writer, context);
    } catch (IOException e) {
      throw new RuntimeException(
          String.format(
              "Could not successfully template a join query to produce the final point-in-time result table. \nContext: %s",
              context),
          e);
    }
    return writer.toString();
  }

  @Override
  protected List<String> generateExportTableSqlQuery(String resultTable, String stagingUri) {
    // support stagingUri with and without a trailing slash
    String exportPath;
    if (stagingUri.substring(stagingUri.length() - 1).equals("/")) {
      exportPath = String.format("%s%s.%s", stagingUri, resultTable, EXPORT_FILE_EXT);
    } else {
      exportPath = String.format("%s/%s.%s", stagingUri, resultTable, EXPORT_FILE_EXT);
    }
    List<String> exportTableSqlQueries = new ArrayList<>();
    String copyIntoStageQuery =
        String.format(
            "COPY INTO '%s' FROM %s file_format = %s\n"
                + "single=true header = true storage_integration = %s;",
            exportPath, resultTable, EXPORT_STAGING_FILE_FORMAT, this.storageIntegration);
    String[] queryArray = new String[] {copyIntoStageQuery};
    exportTableSqlQueries.addAll(Arrays.asList(queryArray));
    return exportTableSqlQueries;
  }
}
