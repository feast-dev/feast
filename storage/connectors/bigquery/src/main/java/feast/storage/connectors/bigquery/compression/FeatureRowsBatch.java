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
package feast.storage.connectors.bigquery.compression;

import static feast.proto.types.ValueProto.Value.ValCase.*;
import static feast.storage.connectors.bigquery.common.TypeUtil.*;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Timestamp;
import feast.proto.types.FeatureRowProto;
import feast.proto.types.FieldProto;
import feast.proto.types.ValueProto;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

/**
 * Based on list of featureRows this class infers common schema (with all found features) and then
 * transpose list of rows into list of values format (column-oriented).
 *
 * <p>getFeatureRows provides reverse transformation
 */
public class FeatureRowsBatch implements Serializable {
  public static final ImmutableList<String> SERVICE_FIELDS =
      ImmutableList.of("eventTimestamp", "ingestionId");
  private final Schema schema;
  private String featureSetReference;
  private List<Object> values = new ArrayList<>();

  public static Map<ValueProto.Value.ValCase, Schema.FieldType> protoToSchemaTypes =
      new HashMap<>();
  public static Map<Schema.FieldType, ValueProto.Value.ValCase> schemaToProtoTypes;
  public static Map<String, Object> defaultValues = new HashMap<>();

  static {
    protoToSchemaTypes.put(BYTES_VAL, Schema.FieldType.BYTES);
    protoToSchemaTypes.put(STRING_VAL, Schema.FieldType.STRING);
    protoToSchemaTypes.put(INT32_VAL, Schema.FieldType.INT32);
    protoToSchemaTypes.put(INT64_VAL, Schema.FieldType.INT64);
    protoToSchemaTypes.put(DOUBLE_VAL, Schema.FieldType.DOUBLE);
    protoToSchemaTypes.put(FLOAT_VAL, Schema.FieldType.FLOAT);
    protoToSchemaTypes.put(BOOL_VAL, Schema.FieldType.BOOLEAN);
    protoToSchemaTypes.put(BYTES_LIST_VAL, Schema.FieldType.array(Schema.FieldType.BYTES));
    protoToSchemaTypes.put(STRING_LIST_VAL, Schema.FieldType.array(Schema.FieldType.STRING));
    protoToSchemaTypes.put(INT32_LIST_VAL, Schema.FieldType.array(Schema.FieldType.INT32));
    protoToSchemaTypes.put(INT64_LIST_VAL, Schema.FieldType.array(Schema.FieldType.INT64));
    protoToSchemaTypes.put(FLOAT_LIST_VAL, Schema.FieldType.array(Schema.FieldType.FLOAT));
    protoToSchemaTypes.put(BOOL_LIST_VAL, Schema.FieldType.array(Schema.FieldType.BOOLEAN));
    protoToSchemaTypes.put(DOUBLE_LIST_VAL, Schema.FieldType.array(Schema.FieldType.DOUBLE));

    schemaToProtoTypes =
        protoToSchemaTypes.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
  }

  public FeatureRowsBatch(Iterable<FeatureRowProto.FeatureRow> featureRows) {
    this.schema = this.inferCommonSchema(featureRows);
    this.initValues();
    this.toColumnar(featureRows);
  }

  FeatureRowsBatch(Schema schema, List<Object> values) {
    this.schema = schema;
    this.values = values;
  }

  private Schema inferCommonSchema(Iterable<FeatureRowProto.FeatureRow> featureRows) {
    Map<String, Schema.FieldType> types = new HashMap<>();
    List<Schema.Field> fieldsInOrder = new ArrayList<>();

    featureRows.forEach(
        row ->
            row.getFieldsList()
                .forEach(
                    f -> {
                      Schema.FieldType fieldType =
                          protoToSchemaTypes.get(f.getValue().getValCase());
                      if (fieldType == null) {
                        return;
                      }
                      if (types.containsKey(f.getName())) {
                        if (!types.get(f.getName()).equals(fieldType)) {
                          throw new RuntimeException("schema cannot be inferred");
                        }

                        return;
                      }

                      if (!defaultValues.containsKey(f.getName())) {
                        defaultValues.put(
                            f.getName(), getDefaultProtoValue(f.getValue().getValCase()));
                      }

                      Schema.Field column =
                          Schema.Field.of(f.getName(), Schema.FieldType.array(fieldType));

                      types.put(f.getName(), fieldType);
                      fieldsInOrder.add(column);

                      if (featureSetReference == null) {
                        featureSetReference = row.getFeatureSet();
                      }
                    }));

    fieldsInOrder.add(
        Schema.Field.of("eventTimestamp", Schema.FieldType.array(Schema.FieldType.INT64)));
    fieldsInOrder.add(
        Schema.Field.of("ingestionId", Schema.FieldType.array(Schema.FieldType.STRING)));

    Schema schema = Schema.builder().addFields(fieldsInOrder).build();
    schema.setUUID(UUID.randomUUID());
    return schema;
  }

  private void initValues() {
    IntStream.range(0, schema.getFieldCount())
        .forEach(
            idx -> {
              values.add(new ArrayList<>());
            });
  }

  private void toColumnar(Iterable<FeatureRowProto.FeatureRow> featureRows) {
    int timestampColumnIdx = schema.indexOf("eventTimestamp");
    int ingestionIdColumnIdx = schema.indexOf("ingestionId");

    featureRows.forEach(
        row -> {
          Map<String, ValueProto.Value> rowValues;
          try {
            rowValues =
                row.getFieldsList().stream()
                    .collect(
                        Collectors.toMap(FieldProto.Field::getName, FieldProto.Field::getValue));
          } catch (IllegalStateException e) {
            // row contains feature duplicates
            // omitting for now
            return;
          }

          schema
              .getFieldNames()
              .forEach(
                  fieldName -> {
                    if (SERVICE_FIELDS.contains(fieldName)) {
                      return;
                    }
                    Schema.Field field = schema.getField(fieldName);
                    int idx = schema.indexOf(fieldName);

                    if (rowValues.containsKey(field.getName())) {
                      Object o = protoValueToObject(rowValues.get(field.getName()));
                      if (o != null) {
                        ((List<Object>) values.get(idx)).add(o);
                        return;
                      }
                    }

                    ((List<Object>) values.get(idx)).add(defaultValues.get(field.getName()));
                  });

          // adding service fields
          ((List<Object>) values.get(timestampColumnIdx)).add(row.getEventTimestamp().getSeconds());
          ((List<Object>) values.get(ingestionIdColumnIdx)).add(row.getIngestionId());
        });
  }

  public Schema getSchema() {
    return this.schema;
  }

  public String getFeatureSetReference() {
    return this.featureSetReference;
  }

  public FeatureRowsBatch withFeatureSetReference(String featureSetReference) {
    this.featureSetReference = featureSetReference;
    return this;
  }

  public Row toRow() {
    return Row.withSchema(schema).attachValues(values).build();
  }

  public static FeatureRowsBatch fromRow(Row row) {
    return new FeatureRowsBatch(row.getSchema(), row.getValues());
  }

  private FeatureRowProto.FeatureRow restoreFeatureRow(int rowIdx) {
    int timestampColumnIdx = schema.indexOf("eventTimestamp");
    int ingestionIdColumnIdx = schema.indexOf("ingestionId");

    return FeatureRowProto.FeatureRow.newBuilder()
        .setFeatureSet(getFeatureSetReference())
        .setEventTimestamp(
            Timestamp.newBuilder()
                .setSeconds(
                    (long)
                        (((List<Object>) values.get(timestampColumnIdx)).get(rowIdx)))
                .build())
        .setIngestionId(
            (String) (((List<Object>) values.get(ingestionIdColumnIdx)).get(rowIdx)))
        .addAllFields(
            schema.getFieldNames().stream()
                .map(
                    fieldName -> {
                      if (SERVICE_FIELDS.contains(fieldName)) {
                        return null;
                      }
                      int fieldIdx = schema.indexOf(fieldName);

                      return FieldProto.Field.newBuilder()
                          .setName(schema.getField(fieldIdx).getName())
                          .setValue(
                              objectToProtoValue(
                                  ((List<Object>) values.get(fieldIdx)).get(rowIdx),
                                  schemaToProtoTypes.get(
                                      schema
                                          .getField(fieldIdx)
                                          .getType()
                                          .getCollectionElementType())))
                          .build();
                    })
                .filter(Objects::nonNull)
                .collect(Collectors.toList()))
        .build();
  }

  public Iterator<FeatureRowProto.FeatureRow> getFeatureRows() {
    int featureCount = ((List<Object>) values.get(0)).size();

    return IntStream.range(0, featureCount)
        .parallel()
        .mapToObj(this::restoreFeatureRow)
        .iterator();
  }

  public Iterator<FeatureRowProto.FeatureRow> getFeatureRowsSample(int maxCount) {
    int featureCount = ((List<Object>) values.get(0)).size();
    Random rd = new Random(42);

    return IntStream.range(0, featureCount)
        .filter(idx -> rd.nextInt(featureCount) < maxCount)
        .parallel()
        .mapToObj(this::restoreFeatureRow)
        .limit(maxCount)
        .iterator();
  }

  public static class FeatureRowsCoder extends CustomCoder<FeatureRowsBatch> {
    private final Coder<Schema> schemaCoder = SerializableCoder.of(Schema.class);
    private final Coder<String> referenceCoder = StringUtf8Coder.of();

    public static FeatureRowsCoder of() {
      return new FeatureRowsCoder();
    }

    private Coder<Row> getDelegateCoder(Schema schema) {
      return RowCoderGenerator.generate(schema);
    }

    @Override
    public void encode(FeatureRowsBatch value, OutputStream outStream)
        throws CoderException, IOException {
      schemaCoder.encode(value.getSchema(), outStream);
      referenceCoder.encode(value.getFeatureSetReference(), outStream);
      getDelegateCoder(value.getSchema()).encode(value.toRow(), outStream);
    }

    @Override
    public FeatureRowsBatch decode(InputStream inStream) throws CoderException, IOException {
      Schema schema = schemaCoder.decode(inStream);
      String reference = referenceCoder.decode(inStream);
      return FeatureRowsBatch.fromRow(getDelegateCoder(schema).decode(inStream))
          .withFeatureSetReference(reference);
    }
  }
}
