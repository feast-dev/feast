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
package feast.spark.ingestion.delta;

import static feast.common.models.FeatureSet.getFeatureSetStringRef;
import static org.apache.spark.sql.types.DataTypes.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import feast.proto.core.FeatureSetProto.EntitySpec;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.FeatureSetProto.FeatureSpec;
import feast.proto.core.SourceProto.KafkaSourceConfig;
import feast.proto.core.SourceProto.Source;
import feast.proto.core.SourceProto.SourceType;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FeatureRowProto.FeatureRow.Builder;
import feast.proto.types.FieldProto.Field;
import feast.proto.types.ValueProto.BoolList;
import feast.proto.types.ValueProto.BytesList;
import feast.proto.types.ValueProto.DoubleList;
import feast.proto.types.ValueProto.FloatList;
import feast.proto.types.ValueProto.Int32List;
import feast.proto.types.ValueProto.Int64List;
import feast.proto.types.ValueProto.StringList;
import feast.proto.types.ValueProto.Value;
import feast.proto.types.ValueProto.ValueType.Enum;
import java.sql.Timestamp;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FeatureRowToSparkRowTest {

  private static final String KAFKA_HOST = "localhost";
  private static final int KAFKA_PORT = 19092;
  private static final String KAFKA_BOOTSTRAP_SERVERS = KAFKA_HOST + ":" + KAFKA_PORT;
  private static final String KAFKA_TOPIC = "topic_1";
  private FeatureRowToSparkRow sut;
  private FeatureSetSpec featureSetSpec;

  @BeforeEach
  public void setup() {
    sut = new FeatureRowToSparkRow("myjob");
    KafkaSourceConfig kafka =
        KafkaSourceConfig.newBuilder()
            .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
            .setTopic(KAFKA_TOPIC)
            .build();
    featureSetSpec =
        FeatureSetSpec.newBuilder()
            .setName("feature_set")
            .setProject("myproject")
            .addEntities(
                EntitySpec.newBuilder()
                    .setName("entity_id_primary")
                    .setValueType(Enum.INT32)
                    .build())
            .addEntities(
                EntitySpec.newBuilder()
                    .setName("entity_id_secondary")
                    .setValueType(Enum.STRING)
                    .build())
            .addFeatures(featureOfType(Enum.BYTES))
            .addFeatures(featureOfType(Enum.STRING))
            .addFeatures(featureOfType(Enum.INT32))
            .addFeatures(featureOfType(Enum.INT64))
            .addFeatures(featureOfType(Enum.DOUBLE))
            .addFeatures(featureOfType(Enum.FLOAT))
            .addFeatures(featureOfType(Enum.BOOL))
            .addFeatures(featureOfType(Enum.BYTES_LIST))
            .addFeatures(featureOfType(Enum.STRING_LIST))
            .addFeatures(featureOfType(Enum.INT32_LIST))
            .addFeatures(featureOfType(Enum.INT64_LIST))
            .addFeatures(featureOfType(Enum.DOUBLE_LIST))
            .addFeatures(featureOfType(Enum.FLOAT_LIST))
            .addFeatures(featureOfType(Enum.BOOL_LIST))
            .addFeatures(featureOfType(Enum.BOOL_LIST, "f_BOOL_LIST_empty"))
            .addFeatures(featureOfType(Enum.BOOL_LIST, "f_BOOL_LIST_unset"))
            .setSource(
                Source.newBuilder().setType(SourceType.KAFKA).setKafkaSourceConfig(kafka).build())
            .build();
  }

  @Test
  public void buildSchema() throws Exception {
    // Apply
    StructType schema = sut.buildSchema(featureSetSpec);

    // Assert
    StructType expected =
        new StructType(
            new StructField[] {
              createStructField("event_timestamp_day", DateType, false),
              createStructField("event_timestamp", TimestampType, false),
              createStructField("created_timestamp", TimestampType, false),
              createStructField("ingestion_id", StringType, false),
              createStructField("entity_id_primary", IntegerType, true),
              createStructField("entity_id_secondary", StringType, true),
              createStructField("f_BYTES", BinaryType, true),
              createStructField("f_STRING", StringType, true),
              createStructField("f_INT32", IntegerType, true),
              createStructField("f_INT64", LongType, true),
              createStructField("f_DOUBLE", DoubleType, true),
              createStructField("f_FLOAT", FloatType, true),
              createStructField("f_BOOL", BooleanType, true),
              createStructField("f_BYTES_LIST", createArrayType(BinaryType, true), true),
              createStructField("f_STRING_LIST", createArrayType(StringType, true), true),
              createStructField("f_INT32_LIST", createArrayType(IntegerType, true), true),
              createStructField("f_INT64_LIST", createArrayType(LongType, true), true),
              createStructField("f_DOUBLE_LIST", createArrayType(DoubleType, true), true),
              createStructField("f_FLOAT_LIST", createArrayType(FloatType, true), true),
              createStructField("f_BOOL_LIST", createArrayType(BooleanType, true), true),
              createStructField("f_BOOL_LIST_empty", createArrayType(BooleanType, true), true),
              createStructField("f_BOOL_LIST_unset", createArrayType(BooleanType, true), true)
            });
    assertThat(schema, is(expected));
  }

  @Test
  public void apply() throws Exception {
    // Arrange
    Builder builder =
        FeatureRow.newBuilder()
            .setFeatureSet(getFeatureSetStringRef(featureSetSpec))
            .setEventTimestamp(Timestamps.fromMillis(100000000));

    builder.addFields(
        Field.newBuilder()
            .setName("entity_id_primary")
            .setValue(Value.newBuilder().setInt32Val(12)));
    builder.addFields(
        Field.newBuilder()
            .setName("entity_id_secondary")
            .setValue(Value.newBuilder().setStringVal("lorem".toString())));
    builder.addFields(
        Field.newBuilder()
            .setName("f_BYTES")
            .setValue(Value.newBuilder().setBytesVal(ByteString.copyFrom("abcd".getBytes()))));
    builder.addFields(
        Field.newBuilder()
            .setName("f_STRING")
            .setValue(Value.newBuilder().setStringVal("ipsum".toString())));
    builder.addFields(
        Field.newBuilder().setName("f_INT32").setValue(Value.newBuilder().setInt32Val(32)));
    builder.addFields(
        Field.newBuilder().setName("f_INT64").setValue(Value.newBuilder().setInt64Val(128L)));
    builder.addFields(
        Field.newBuilder().setName("f_DOUBLE").setValue(Value.newBuilder().setDoubleVal(3.1415)));
    builder.addFields(
        Field.newBuilder().setName("f_FLOAT").setValue(Value.newBuilder().setFloatVal(7.62f)));
    builder.addFields(
        Field.newBuilder().setName("f_BOOL").setValue(Value.newBuilder().setBoolVal(true)));
    builder.addFields(
        Field.newBuilder()
            .setName("f_BYTES_LIST")
            .setValue(
                Value.newBuilder()
                    .setBytesListVal(
                        BytesList.newBuilder()
                            .addVal(ByteString.copyFrom("efg".getBytes()))
                            .build())));
    builder.addFields(
        Field.newBuilder()
            .setName("f_STRING_LIST")
            .setValue(
                Value.newBuilder()
                    .setStringListVal(StringList.newBuilder().addVal("dolor").addVal("amet"))));
    builder.addFields(
        Field.newBuilder()
            .setName("f_INT32_LIST")
            .setValue(Value.newBuilder().setInt32ListVal(Int32List.newBuilder().addVal(64))));
    builder.addFields(
        Field.newBuilder()
            .setName("f_INT64_LIST")
            .setValue(Value.newBuilder().setInt64ListVal(Int64List.newBuilder().addVal(256L))));
    builder.addFields(
        Field.newBuilder()
            .setName("f_DOUBLE_LIST")
            .setValue(
                Value.newBuilder().setDoubleListVal(DoubleList.newBuilder().addVal(6.14156))));
    builder.addFields(
        Field.newBuilder()
            .setName("f_FLOAT_LIST")
            .setValue(Value.newBuilder().setFloatListVal(FloatList.newBuilder().addVal(8.62f))));
    builder.addFields(
        Field.newBuilder()
            .setName("f_BOOL_LIST")
            .setValue(Value.newBuilder().setBoolListVal(BoolList.newBuilder().addVal(false))));
    builder.addFields(
        Field.newBuilder()
            .setName("f_BOOL_LIST_empty")
            .setValue(Value.newBuilder().setBoolListVal(BoolList.newBuilder())));

    // Apply
    Timestamp before = new java.sql.Timestamp(System.currentTimeMillis());
    Row row = sut.apply(featureSetSpec, builder.build());
    Timestamp after = new java.sql.Timestamp(System.currentTimeMillis());

    // Assert
    assertThat(row.length(), is(22));

    int i = 0;
    // EVENT_TIMESTAMP_DAY_COLUMN
    assertThat(((java.sql.Date) row.get(i++)).getTime(), is(100000000L));
    // EVENT_TIMESTAMP_COLUMN
    assertThat(((java.sql.Timestamp) row.get(i++)).getTime(), is(100000000L));
    // CREATED_TIMESTAMP_COLUMN
    Timestamp value = (java.sql.Timestamp) row.get(i++);
    assertThat(value, greaterThanOrEqualTo(before));
    assertThat(value, lessThanOrEqualTo(after));
    // INGESTION_ID_COLUMN
    assertThat(row.get(i++), is("myjob"));
    // values
    assertThat(row.get(i++), is(12));
    assertThat(row.get(i++), is("lorem"));
    assertThat(row.get(i++), is("abcd".getBytes()));
    assertThat(row.get(i++), is("ipsum"));
    assertThat(row.get(i++), is(32));
    assertThat(row.get(i++), is(128L));
    assertThat(row.get(i++), is(3.1415));
    assertThat(row.get(i++), is(7.62f));
    assertThat(row.get(i++), is(true));
    assertThat(row.get(i++), is(new byte[][] {"efg".getBytes()}));
    assertThat(row.get(i++), is(new String[] {"dolor", "amet"}));
    assertThat(row.get(i++), is(new int[] {64}));
    assertThat(row.get(i++), is(new long[] {256L}));
    assertThat(row.get(i++), is(new double[] {6.14156}));
    assertThat(row.get(i++), is(new float[] {8.62f}));
    assertThat(row.get(i++), is(new boolean[] {false}));
    assertThat(row.get(i++), is(new boolean[] {}));
    assertNull(row.get(i++));
  }

  private static FeatureSpec featureOfType(Enum type) {
    return featureOfType(type, "f_" + type.name());
  }

  private static FeatureSpec featureOfType(Enum type, String name) {
    return FeatureSpec.newBuilder().setName(name).setValueType(type).build();
  }
}
