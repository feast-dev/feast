/*
 * Copyright 2018 The Feast Authors
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
 *
 */

package feast.storage.bigquery;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.common.collect.Maps;
import com.google.protobuf.Timestamp;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.transforms.SerializableFunction;
import feast.ingestion.model.Values;
import feast.specs.ImportSpecProto.Field;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.types.FeatureProto.Feature;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.ValueProto.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a serializable function used with the BigQueryIO for fetching feature rows directly from
 * BigQuery
 */
public class FeatureRowFromBigQuerySchemaAndRecordFn
    implements SerializableFunction<SchemaAndRecord, FeatureRow> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FeatureRowFromBigQuerySchemaAndRecordFn.class);

  private final ImportSpec importSpec;
  private final Map<String, Field> fields;

  public FeatureRowFromBigQuerySchemaAndRecordFn(ImportSpec importSpec) {
    this.importSpec = importSpec;
    fields = Maps.newHashMap();
    for (Field field : importSpec.getSchema().getFieldsList()) {
      fields.put(field.getName().isEmpty() ? field.getFeatureId() : field.getName(), field);
    }
  }

  @Override
  public FeatureRow apply(SchemaAndRecord input) {
    GenericRecord record = input.getRecord();
    TableSchema schema = input.getTableSchema();

    FeatureRow.Builder builder = FeatureRow.newBuilder();
    builder.setEntityName(importSpec.getEntities(0));

    String entityKeyColumn = importSpec.getSchema().getEntityIdColumn();
    String timestampColumn = importSpec.getSchema().getTimestampColumn();

    for (TableFieldSchema tableFieldSchema : schema.getFields()) {
      String name = tableFieldSchema.getName();
      String bigQueryType = tableFieldSchema.getType();

      if (!fields.containsKey(name)) {
        continue;
      }
      Field field = fields.get(name);

      Value value;
      StandardSQLTypeName bqType = LegacySQLTypeName.valueOfStrict(bigQueryType).getStandardType();
      value = ValueBigQueryBuilder.valueOf(record.get(name), bqType);

      if (name.equals(entityKeyColumn)) {
        builder.setEntityKey(Values.asString(value).getStringVal());
      } else if (name.equals(timestampColumn)) {
        builder.setEventTimestamp(value.getTimestampVal());
      } else if (!field.getFeatureId().isEmpty()) {
        builder.addFeatures(Feature.newBuilder().setId(field.getFeatureId()).setValue(value));
      }
    }
    if (!importSpec.getSchema().getTimestampValue().equals(Timestamp.getDefaultInstance())) {
      builder.setEventTimestamp(importSpec.getSchema().getTimestampValue());
    }
    return builder.build();
  }
}
