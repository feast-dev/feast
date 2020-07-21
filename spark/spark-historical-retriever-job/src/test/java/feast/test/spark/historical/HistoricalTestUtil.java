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
package feast.test.spark.historical;

import static feast.common.models.FeatureSet.getFeatureSetStringRef;
import static org.hamcrest.Matchers.startsWith;

import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.SourceProto;
import feast.proto.types.FeatureRowProto;
import feast.proto.types.FieldProto;
import feast.proto.types.ValueProto;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.spark.sql.SparkSession;
import org.junit.Assume;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HistoricalTestUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(HistoricalTestUtil.class.getName());

  public static FeatureSetProto.FeatureSet createFeatureSetForDelta(
      String fsName, String columnPrefix) {
    SourceProto.KafkaSourceConfig sourceConfig =
        SourceProto.KafkaSourceConfig.newBuilder()
            .setBootstrapServers("none")
            .setTopic("none")
            .build();
    FeatureSetProto.FeatureSetSpec spec1 =
        FeatureSetProto.FeatureSetSpec.newBuilder()
            .setName(fsName)
            .setProject("myproject")
            .addEntities(
                FeatureSetProto.EntitySpec.newBuilder()
                    .setName("entity_id_primary")
                    .setValueType(ValueProto.ValueType.Enum.INT32)
                    .build())
            .addFeatures(featureOfType(columnPrefix, ValueProto.ValueType.Enum.BYTES))
            .addFeatures(featureOfType(columnPrefix, ValueProto.ValueType.Enum.STRING))
            .addFeatures(featureOfType(columnPrefix, ValueProto.ValueType.Enum.INT32))
            .addFeatures(featureOfType(columnPrefix, ValueProto.ValueType.Enum.INT64))
            .addFeatures(featureOfType(columnPrefix, ValueProto.ValueType.Enum.DOUBLE))
            .addFeatures(featureOfType(columnPrefix, ValueProto.ValueType.Enum.FLOAT))
            .addFeatures(featureOfType(columnPrefix, ValueProto.ValueType.Enum.BOOL))
            // FIXME causes roundtrip assertion error with Spark
            // .addFeatures(featureOfType(Enum.BYTES_LIST))
            .addFeatures(featureOfType(columnPrefix, ValueProto.ValueType.Enum.STRING_LIST))
            .addFeatures(featureOfType(columnPrefix, ValueProto.ValueType.Enum.INT32_LIST))
            .addFeatures(featureOfType(columnPrefix, ValueProto.ValueType.Enum.INT64_LIST))
            .addFeatures(featureOfType(columnPrefix, ValueProto.ValueType.Enum.DOUBLE_LIST))
            .addFeatures(featureOfType(columnPrefix, ValueProto.ValueType.Enum.FLOAT_LIST))
            .addFeatures(featureOfType(columnPrefix, ValueProto.ValueType.Enum.BOOL_LIST))
            .setSource(
                SourceProto.Source.newBuilder()
                    .setType(SourceProto.SourceType.KAFKA)
                    .setKafkaSourceConfig(sourceConfig)
                    .build())
            .build();

    FeatureSetProto.FeatureSet featureSet =
        FeatureSetProto.FeatureSet.newBuilder().setSpec(spec1).build();
    return featureSet;
  }

  private static FeatureSetProto.FeatureSpec featureOfType(
      String prefix, ValueProto.ValueType.Enum type) {
    return FeatureSetProto.FeatureSpec.newBuilder()
        .setName(prefix + type.name())
        .setValueType(type)
        .build();
  }

  public static List<FeatureRowProto.FeatureRow> generateTestData(
      FeatureSetProto.FeatureSetSpec spec, int size) {
    LOGGER.info("Generating test data ...");
    List<FeatureRowProto.FeatureRow> input = new ArrayList<>();
    IntStream.range(0, size)
        .forEach(
            i -> {
              FeatureRowProto.FeatureRow randomRow =
                  HistoricalTestUtil.createRandomFeatureRow(spec, i);
              input.add(randomRow);
            });
    return input;
  }

  /**
   * Create a Feature Row with random value according to the FeatureSetSpec
   *
   * <p>See {@link #createRandomFeatureRow(FeatureSetProto.FeatureSetSpec, int)}
   *
   * @param featureSetSpec {@link FeatureSetProto.FeatureSetSpec}
   * @return {@link FeatureRowProto.FeatureRow}
   */
  public static FeatureRowProto.FeatureRow createRandomFeatureRow(
      FeatureSetProto.FeatureSetSpec featureSetSpec, int row_id) {
    ThreadLocalRandom random = ThreadLocalRandom.current();
    int randomStringSizeMaxSize = 12;
    return createRandomFeatureRow(
        featureSetSpec, row_id, random.nextInt(0, randomStringSizeMaxSize) + 4);
  }

  /**
   * Create a Feature Row with random value according to the FeatureSet.
   *
   * <p>The Feature Row created contains fields according to the entities and features defined in
   * FeatureSet, matching the value type of the field, with randomized value for testing. Entity IDs
   *
   * @param featureSetSpec {@link FeatureSetProto.FeatureSetSpec}
   * @param randomStringSize number of characters for the generated random string
   * @return {@link FeatureRowProto.FeatureRow}
   */
  public static FeatureRowProto.FeatureRow createRandomFeatureRow(
      FeatureSetProto.FeatureSetSpec featureSetSpec, int rowId, int randomStringSize) {
    FeatureRowProto.FeatureRow.Builder builder =
        FeatureRowProto.FeatureRow.newBuilder()
            .setFeatureSet(getFeatureSetStringRef(featureSetSpec))
            .setEventTimestamp(Timestamps.fromMillis(System.currentTimeMillis()));

    builder.addFields(
        FieldProto.Field.newBuilder()
            .setValue(ValueProto.Value.newBuilder().setInt32Val(rowId))
            .setName(featureSetSpec.getEntities(0).getName()));

    featureSetSpec
        .getFeaturesList()
        .forEach(
            field -> {
              builder.addFields(
                  FieldProto.Field.newBuilder()
                      .setName(field.getName())
                      .setValue(createRandomValue(field.getValueType(), randomStringSize))
                      .build());
            });

    return builder.build();
  }

  /**
   * Create a random Feast {@link ValueProto.Value} of {@link ValueProto.ValueType.Enum}.
   *
   * @param type {@link ValueProto.ValueType.Enum}
   * @param randomStringSize number of characters for the generated random string
   * @return {@link ValueProto.Value}
   */
  public static ValueProto.Value createRandomValue(
      ValueProto.ValueType.Enum type, int randomStringSize) {
    ValueProto.Value.Builder builder = ValueProto.Value.newBuilder();
    ThreadLocalRandom random = ThreadLocalRandom.current();

    switch (type) {
      case INVALID:
      case UNRECOGNIZED:
        throw new IllegalArgumentException("Invalid ValueType: " + type);
      case BYTES:
        builder.setBytesVal(
            ByteString.copyFrom(RandomStringUtils.randomAlphanumeric(randomStringSize).getBytes()));
        break;
      case STRING:
        builder.setStringVal(RandomStringUtils.randomAlphanumeric(randomStringSize));
        break;
      case INT32:
        builder.setInt32Val(random.nextInt());
        break;
      case INT64:
        builder.setInt64Val(random.nextLong());
        break;
      case DOUBLE:
        builder.setDoubleVal(random.nextDouble());
        break;
      case FLOAT:
        builder.setFloatVal(random.nextFloat());
        break;
      case BOOL:
        builder.setBoolVal(random.nextBoolean());
        break;
      case BYTES_LIST:
        builder.setBytesListVal(
            ValueProto.BytesList.newBuilder()
                .addVal(
                    ByteString.copyFrom(
                        RandomStringUtils.randomAlphanumeric(randomStringSize).getBytes()))
                .build());
        break;
      case STRING_LIST:
        builder.setStringListVal(
            ValueProto.StringList.newBuilder()
                .addVal(RandomStringUtils.randomAlphanumeric(randomStringSize))
                .build());
        break;
      case INT32_LIST:
        builder.setInt32ListVal(ValueProto.Int32List.newBuilder().addVal(random.nextInt()).build());
        break;
      case INT64_LIST:
        builder.setInt64ListVal(
            ValueProto.Int64List.newBuilder().addVal(random.nextLong()).build());
        break;
      case DOUBLE_LIST:
        builder.setDoubleListVal(
            ValueProto.DoubleList.newBuilder().addVal(random.nextDouble()).build());
        break;
      case FLOAT_LIST:
        builder.setFloatListVal(
            ValueProto.FloatList.newBuilder().addVal(random.nextFloat()).build());
        break;
      case BOOL_LIST:
        builder.setBoolListVal(
            ValueProto.BoolList.newBuilder().addVal(random.nextBoolean()).build());
        break;
    }
    return builder.build();
  }

  public final class SparkSessionRule implements TestRule {
    public SparkSession session;

    @Override
    public Statement apply(final Statement base, final Description description) {
      return new Statement() {
        @Override
        public void evaluate() throws Throwable {
          Assume.assumeThat(System.getProperty("java.version"), startsWith("1.8"));
          session =
              SparkSession.builder().appName(getClass().getName()).master("local").getOrCreate();
          try {
            base.evaluate(); // This will run the test.
          } finally {
            session.close();
          }
        }
      };
    }
  }
}
