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
package feast.spark.ingestion.delta;

import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import feast.proto.core.FeatureSetProto.EntitySpec;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.FeatureSetProto.FeatureSpec;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.ValueProto;
import feast.proto.types.ValueProto.Value;
import feast.proto.types.ValueProto.ValueType;
import feast.proto.types.ValueProto.ValueType.Enum;
import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.*;

// TODO: Validate FeatureRow against FeatureSetSpec
//       i.e. that the value types in FeatureRow matches against those in FeatureSetSpec

@SuppressWarnings("serial")
/** Converts Protocol Buffers FeatureRow into Spark Rows. */
public class FeatureRowToSparkRow implements Serializable {
  public static final String EVENT_TIMESTAMP_DAY_COLUMN = "event_timestamp_day";
  public static final String EVENT_TIMESTAMP_COLUMN = "event_timestamp";
  public static final String CREATED_TIMESTAMP_COLUMN = "created_timestamp";
  public static final String INGESTION_ID_COLUMN = "ingestion_id";
  private final String jobId;

  public FeatureRowToSparkRow(String jobId) {
    this.jobId = jobId;
  }

  /**
   * Builds a Spark DataFrame schema from a FeatureSetSpec.
   *
   * @param spec FeatureSetSpec to be mapped.
   * @return A Spark DataFrame schema.
   */
  public StructType buildSchema(FeatureSetSpec spec) {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField(EVENT_TIMESTAMP_DAY_COLUMN, DataTypes.DateType, false),
              DataTypes.createStructField(EVENT_TIMESTAMP_COLUMN, DataTypes.TimestampType, false),
              DataTypes.createStructField(CREATED_TIMESTAMP_COLUMN, DataTypes.TimestampType, false),
              DataTypes.createStructField(INGESTION_ID_COLUMN, DataTypes.StringType, false),
            });
    for (EntitySpec entity : spec.getEntitiesList()) {
      schema = addStructField(schema, entity.getName(), entity.getValueType());
    }
    for (FeatureSpec feature : spec.getFeaturesList()) {
      schema = addStructField(schema, feature.getName(), feature.getValueType());
    }
    return schema;
  }

  private StructType addStructField(StructType schema, String name, Enum type) {
    return schema.add(DataTypes.createStructField(name, protobufTypeToSparkType(type), true));
  }

  private DataType protobufTypeToSparkType(ValueType.Enum type) {
    switch (type) {
      case BOOL:
        return DataTypes.BooleanType;
      case BOOL_LIST:
        return DataTypes.createArrayType(DataTypes.BooleanType);
      case BYTES:
        return DataTypes.BinaryType;
      case BYTES_LIST:
        return DataTypes.createArrayType(DataTypes.BinaryType);
      case DOUBLE:
        return DataTypes.DoubleType;
      case DOUBLE_LIST:
        return DataTypes.createArrayType(DataTypes.DoubleType);
      case FLOAT:
        return DataTypes.FloatType;
      case FLOAT_LIST:
        return DataTypes.createArrayType(DataTypes.FloatType);
      case INT32:
        return DataTypes.IntegerType;
      case INT32_LIST:
        return DataTypes.createArrayType(DataTypes.IntegerType);
      case INT64:
        return DataTypes.LongType;
      case INT64_LIST:
        return DataTypes.createArrayType(DataTypes.LongType);
      case STRING:
        return DataTypes.StringType;
      case STRING_LIST:
        return DataTypes.createArrayType(DataTypes.StringType);
      default:
        throw new UnsupportedOperationException("Not implemented type " + type);
    }
  }

  /**
   * Generate a Spark DataFrame row based on a FeatureSetSpec and values from a FeatureRow. Missing
   * or absent values in featureRow are mapped as null.
   *
   * @param spec FeatureSetSpec corresponding to featureRow.
   * @param featureRow entity and feature values.
   * @return a Spark DataFrame row.
   */
  public Row apply(FeatureSetSpec spec, FeatureRow featureRow) {

    Map<String, ValueProto.Value> fields =
        featureRow.getFieldsList().stream()
            .collect(Collectors.toMap(f -> f.getName(), f -> f.getValue()));

    Object[] values = new Object[4 + spec.getEntitiesCount() + spec.getFeaturesCount()];
    int p = 0;

    com.google.protobuf.Timestamp ts = featureRow.getEventTimestamp();
    // EVENT_TIMESTAMP_DAY_COLUMN
    values[p++] = new java.sql.Date(Timestamps.toMillis(ts));
    // EVENT_TIMESTAMP_COLUMN
    values[p++] = java.sql.Timestamp.from(Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos()));
    // CREATED_TIMESTAMP_COLUMN
    values[p++] = java.sql.Timestamp.from(Instant.now());
    // INGESTION_ID_COLUMN
    values[p++] = jobId;

    for (EntitySpec entity : spec.getEntitiesList()) {
      values[p++] = mapValue(fields.get(entity.getName()));
    }
    for (FeatureSpec feature : spec.getFeaturesList()) {
      values[p++] = mapValue(fields.get(feature.getName()));
    }

    return RowFactory.create(values);
  }

  private Object mapValue(Value value) {
    if (value == null) {
      return null;
    }
    switch (value.getValCase()) {
      case BYTES_VAL:
        return value.getBytesVal().toByteArray();
      case STRING_VAL:
        return value.getStringVal();
      case INT32_VAL:
        return value.getInt32Val();
      case INT64_VAL:
        return value.getInt64Val();
      case DOUBLE_VAL:
        return value.getDoubleVal();
      case FLOAT_VAL:
        return value.getFloatVal();
      case BOOL_VAL:
        return value.getBoolVal();
      case BYTES_LIST_VAL:
        return value.getBytesListVal().getValList().stream()
            .map(ByteString::toByteArray)
            .toArray(byte[][]::new);
      case STRING_LIST_VAL:
        return value.getStringListVal().getValList().toArray(new String[0]);
      case INT32_LIST_VAL:
        return value.getInt32ListVal().getValList().toArray(new Integer[0]);
      case INT64_LIST_VAL:
        return value.getInt64ListVal().getValList().toArray(new Long[0]);
      case DOUBLE_LIST_VAL:
        return value.getDoubleListVal().getValList().toArray(new Double[0]);
      case FLOAT_LIST_VAL:
        return value.getFloatListVal().getValList().toArray(new Float[0]);
      case BOOL_LIST_VAL:
        return value.getBoolListVal().getValList().toArray(new Boolean[0]);
      case VAL_NOT_SET:
        return null;
      default:
        throw new UnsupportedOperationException("Unknown type: " + value.getValCase());
    }
  }
}
