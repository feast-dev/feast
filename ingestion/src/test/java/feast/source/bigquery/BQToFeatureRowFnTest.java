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

package feast.source.bigquery;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.common.collect.Lists;
import com.google.protobuf.Timestamp;
import feast.source.bigquery.BigQueryToFeatureRowFn;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;
import feast.ingestion.model.Features;
import feast.ingestion.model.Values;
import feast.ingestion.util.DateUtil;
import feast.specs.ImportSpecProto.Field;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.specs.ImportSpecProto.Schema;
import feast.types.FeatureRowProto.FeatureRow;

public class BQToFeatureRowFnTest {
  @Test
  public void testImportSpecFieldsMissingFromBQTable() {
    // TODO what if a field in the import spec is not in the bq schema
  }

  @Test
  public void testStringEntityKey() {
    Timestamp now = DateUtil.toTimestamp(DateTime.now());
    ImportSpec importSpec =
        ImportSpec.newBuilder()
            .setType("bigquery")
            .addEntities("testEntity")
            .setSchema(
                Schema.newBuilder()
                    .setTimestampValue(now)
                    .setEntityIdColumn("bq_id")
                    .addFields(Field.newBuilder().setName("bq_timestamp"))
                    .addFields(Field.newBuilder().setName("bq_id"))
                    .addFields(
                        Field.newBuilder()
                            .setName("bq_value")
                            .setFeatureId("testEntity.day.testInt64")))
            .build();

    GenericRecord record = mock(GenericRecord.class);
    when(record.get("bq_id")).thenReturn("abcd");
    when(record.get("bq_timestamp"))
        .thenReturn(now.getSeconds() * 1000000); // BQ uses Long in microseconds
    when(record.get("bq_value")).thenReturn(Long.MAX_VALUE);
    TableSchema tableSchema = new TableSchema();
    // Type names are strings that must match LegacySQLTypeName.class
    tableSchema.setFields(
        Lists.newArrayList(
            new TableFieldSchema().setName("bq_id").setType(LegacySQLTypeName.STRING.name()),
            new TableFieldSchema().setName("bq_timestamp").setType(LegacySQLTypeName.TIMESTAMP.name()),
            new TableFieldSchema().setName("bq_value").setType(LegacySQLTypeName.INTEGER.name())));
    SchemaAndRecord schemaAndRecord = new SchemaAndRecord(record, tableSchema);
    FeatureRow row = new BigQueryToFeatureRowFn(importSpec).apply(schemaAndRecord);
    Assert.assertEquals(now, row.getEventTimestamp());
    Assert.assertEquals("abcd", row.getEntityKey());
    Assert.assertEquals("testEntity", row.getEntityName());
    Assert.assertThat(
        row.getFeaturesList(),
        equalTo(
            Lists.newArrayList(
                Features.of("testEntity.day.testInt64", Values.ofInt64(Long.MAX_VALUE)))));
  }

  @Test
  public void testInt64EntityKey() {
    Timestamp now = DateUtil.toTimestamp(DateTime.now());
    ImportSpec importSpec =
        ImportSpec.newBuilder()
            .setType("bigquery")
            .addEntities("testEntity")
            .setSchema(
                Schema.newBuilder()
                    .setTimestampValue(now)
                    .setEntityIdColumn("bq_id")
                    .addFields(Field.newBuilder().setName("bq_timestamp"))
                    .addFields(Field.newBuilder().setName("bq_id"))
                    .addFields(
                        Field.newBuilder()
                            .setName("bq_value")
                            .setFeatureId("testEntity.day.testInt64")))
            .build();

    GenericRecord record = mock(GenericRecord.class);
    when(record.get("bq_id")).thenReturn(1234L);
    when(record.get("bq_timestamp"))
        .thenReturn(now.getSeconds() * 1000000); // BQ uses Long in microseconds
    when(record.get("bq_value")).thenReturn(Long.MAX_VALUE);
    TableSchema tableSchema = new TableSchema();
    // Type names are strings that must match LegacySQLTypeName.class
    tableSchema.setFields(
        Lists.newArrayList(
            new TableFieldSchema().setName("bq_id").setType(LegacySQLTypeName.INTEGER.name()),
            new TableFieldSchema().setName("bq_timestamp").setType(LegacySQLTypeName.TIMESTAMP.name()),
            new TableFieldSchema().setName("bq_value").setType(LegacySQLTypeName.INTEGER.name())));
    SchemaAndRecord schemaAndRecord = new SchemaAndRecord(record, tableSchema);
    FeatureRow row = new BigQueryToFeatureRowFn(importSpec).apply(schemaAndRecord);
    Assert.assertEquals(now, row.getEventTimestamp());
    Assert.assertEquals("1234", row.getEntityKey());
    Assert.assertEquals("testEntity", row.getEntityName());
    Assert.assertThat(
        row.getFeaturesList(),
        equalTo(
            Lists.newArrayList(
                Features.of("testEntity.day.testInt64", Values.ofInt64(Long.MAX_VALUE)))));
  }
}
