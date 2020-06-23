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
package feast.storage.connectors.bigquery.writer;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import feast.common.models.FeatureSetReference;
import feast.proto.core.FeatureSetProto;
import feast.storage.connectors.bigquery.common.TypeUtil;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;

/**
 * Converts {@link feast.proto.core.FeatureSetProto.FeatureSetSpec} into BigQuery schema. Serializes
 * it into json-like format {@link TableSchema}. Fetches existing schema to merge existing fields
 * with new ones.
 *
 * <p>As a side effect this Operation may create bq table (if it doesn't exist) to make
 * bootstrapping faster
 */
public class FeatureSetSpecToTableSchema
    extends DoFn<
        KV<FeatureSetReference, FeatureSetProto.FeatureSetSpec>,
        KV<FeatureSetReference, TableSchema>> {
  private BigQuery bqService;
  private DatasetId dataset;
  private ValueProvider<BigQuery> bqProvider;

  private static final Logger log =
      org.slf4j.LoggerFactory.getLogger(FeatureSetSpecToTableSchema.class);

  // Reserved columns
  public static final String EVENT_TIMESTAMP_COLUMN = "event_timestamp";
  public static final String CREATED_TIMESTAMP_COLUMN = "created_timestamp";
  public static final String INGESTION_ID_COLUMN = "ingestion_id";
  public static final String JOB_ID_COLUMN = "job_id";

  // Column description for reserved fields
  public static final String BIGQUERY_EVENT_TIMESTAMP_FIELD_DESCRIPTION =
      "Event time for the FeatureRow";
  public static final String BIGQUERY_CREATED_TIMESTAMP_FIELD_DESCRIPTION =
      "Processing time of the FeatureRow ingestion in Feast\"";
  public static final String BIGQUERY_INGESTION_ID_FIELD_DESCRIPTION =
      "Unique id identifying groups of rows that have been ingested together";
  public static final String BIGQUERY_JOB_ID_FIELD_DESCRIPTION =
      "Feast import job ID for the FeatureRow";

  public FeatureSetSpecToTableSchema(DatasetId dataset, ValueProvider<BigQuery> bqProvider) {
    this.dataset = dataset;
    this.bqProvider = bqProvider;
  }

  @Setup
  public void setup() {
    this.bqService = bqProvider.get();
  }

  @ProcessElement
  public void processElement(
      @Element KV<FeatureSetReference, FeatureSetProto.FeatureSetSpec> element,
      OutputReceiver<KV<FeatureSetReference, TableSchema>> output,
      ProcessContext context) {
    String specKey = element.getKey().getReference();

    Table existingTable = getExistingTable(specKey);
    Schema schema = createSchemaFromSpec(element.getValue(), specKey, existingTable);

    if (existingTable == null) {
      createTable(specKey, schema);
    }

    output.output(KV.of(element.getKey(), serializeSchema(schema)));
  }

  private TableId generateTableId(String specKey) {
    TableDestination tableDestination = BigQuerySinkHelpers.getTableDestination(dataset, specKey);
    TableReference tableReference = BigQueryHelpers.parseTableSpec(tableDestination.getTableSpec());
    return TableId.of(
        tableReference.getProjectId(), tableReference.getDatasetId(), tableReference.getTableId());
  }

  private Table getExistingTable(String specKey) {
    return bqService.getTable(generateTableId(specKey));
  }

  private void createTable(String specKey, Schema schema) {
    TimePartitioning timePartitioning =
        TimePartitioning.newBuilder(TimePartitioning.Type.DAY)
            .setField(EVENT_TIMESTAMP_COLUMN)
            .build();

    StandardTableDefinition tableDefinition =
        StandardTableDefinition.newBuilder()
            .setTimePartitioning(timePartitioning)
            .setSchema(schema)
            .build();

    TableInfo tableInfo = TableInfo.of(generateTableId(specKey), tableDefinition);

    bqService.create(tableInfo);
  }

  /**
   * Creates a BigQuery {@link Schema} based on the provided FeatureSetSpec and the existing table,
   * if any. If a table already exists, existing fields will be retained, and new fields present in
   * the feature set will be appended to the existing FieldsList.
   *
   * @param spec FeatureSet spec that this table is for
   * @param specKey String for retrieving existing table
   * @param existingTable Table fetched from BQ. Fields from existing table used to merge with new
   *     schema
   * @return {@link Schema} containing all tombstoned and active fields.
   */
  private Schema createSchemaFromSpec(
      FeatureSetProto.FeatureSetSpec spec, String specKey, Table existingTable) {
    List<Field> fields = new ArrayList<>();
    log.info("Table {} will have the following fields:", specKey);

    for (FeatureSetProto.EntitySpec entitySpec : spec.getEntitiesList()) {
      Field.Builder builder =
          Field.newBuilder(
              entitySpec.getName(), TypeUtil.toStandardSqlType(entitySpec.getValueType()));
      if (entitySpec.getValueType().name().toLowerCase().endsWith("_list")) {
        builder.setMode(Field.Mode.REPEATED);
      }
      Field field = builder.build();
      log.info("- {}", field.toString());
      fields.add(field);
    }
    for (FeatureSetProto.FeatureSpec featureSpec : spec.getFeaturesList()) {
      Field.Builder builder =
          Field.newBuilder(
              featureSpec.getName(), TypeUtil.toStandardSqlType(featureSpec.getValueType()));
      if (featureSpec.getValueType().name().toLowerCase().endsWith("_list")) {
        builder.setMode(Field.Mode.REPEATED);
      }

      Field field = builder.build();
      log.info("- {}", field.toString());
      fields.add(field);
    }

    // Refer to protos/feast/core/Store.proto for reserved fields in BigQuery.
    Map<String, Pair<StandardSQLTypeName, String>>
        reservedFieldNameToPairOfStandardSQLTypeAndDescription =
            ImmutableMap.of(
                EVENT_TIMESTAMP_COLUMN,
                Pair.of(StandardSQLTypeName.TIMESTAMP, BIGQUERY_EVENT_TIMESTAMP_FIELD_DESCRIPTION),
                CREATED_TIMESTAMP_COLUMN,
                Pair.of(
                    StandardSQLTypeName.TIMESTAMP, BIGQUERY_CREATED_TIMESTAMP_FIELD_DESCRIPTION),
                INGESTION_ID_COLUMN,
                Pair.of(StandardSQLTypeName.STRING, BIGQUERY_INGESTION_ID_FIELD_DESCRIPTION),
                JOB_ID_COLUMN,
                Pair.of(StandardSQLTypeName.STRING, BIGQUERY_JOB_ID_FIELD_DESCRIPTION));
    for (Map.Entry<String, Pair<StandardSQLTypeName, String>> entry :
        reservedFieldNameToPairOfStandardSQLTypeAndDescription.entrySet()) {
      Field field =
          Field.newBuilder(entry.getKey(), entry.getValue().getLeft())
              .setDescription(entry.getValue().getRight())
              .build();
      log.info("- {}", field.toString());
      fields.add(field);
    }

    List<Field> fieldsList = new ArrayList<>();
    if (existingTable != null) {
      Schema existingSchema = existingTable.getDefinition().getSchema();
      fieldsList.addAll(existingSchema.getFields());
    }

    for (Field field : fields) {
      if (!fieldsList.contains(field)) {
        fieldsList.add(field);
        log.info("- {}", field.toString());
      }
    }

    return Schema.of(FieldList.of(fieldsList));
  }

  /**
   * Convert Table schema into json-like object (prepared for serialization)
   *
   * @param schema bq table schema
   * @return json-like schema
   */
  private TableSchema serializeSchema(Schema schema) {
    TableSchema tableSchema = new TableSchema();
    FieldList fields = schema.getFields();
    List<TableFieldSchema> tableFieldSchemas =
        fields.stream()
            .map(
                field -> {
                  TableFieldSchema f =
                      new TableFieldSchema()
                          .setName(field.getName())
                          .setType(field.getType().name());

                  if (field.getMode() != null) {
                    f.setMode(field.getMode().name());
                  }

                  if (field.getDescription() != null) {
                    f.setDescription(field.getDescription());
                  }
                  return f;
                })
            .collect(Collectors.toList());

    tableSchema.setFields(tableFieldSchemas);
    return tableSchema;
  }

  public static class TableSchemaCoder extends Coder<TableSchema> {
    private static final StringUtf8Coder stringCoder = StringUtf8Coder.of();
    private static final TableSchemaCoder INSTANCE = new TableSchemaCoder();

    public static TableSchemaCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(TableSchema value, OutputStream outStream)
        throws CoderException, IOException {
      stringCoder.encode(BigQueryHelpers.toJsonString(value), outStream);
    }

    @Override
    public TableSchema decode(InputStream inStream) throws CoderException, IOException {
      return BigQueryHelpers.fromJsonString(stringCoder.decode(inStream), TableSchema.class);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return ImmutableList.of();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {}
  }
}
