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
package feast.storage.connectors.bigquery.stats;

import static feast.storage.connectors.bigquery.stats.StatsUtil.toFeatureNameStatistics;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValue.Attribute;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.types.ValueProto.ValueType.Enum;
import org.junit.Test;
import org.tensorflow.metadata.v0.FeatureNameStatistics;

public class StatsUtilTest {

  private Schema basicStatsSchema =
      Schema.of(
          com.google.cloud.bigquery.Field.of("feature_name", LegacySQLTypeName.STRING),
          com.google.cloud.bigquery.Field.of("total_count", LegacySQLTypeName.INTEGER),
          com.google.cloud.bigquery.Field.of("feature_count", LegacySQLTypeName.INTEGER),
          com.google.cloud.bigquery.Field.of("missing_count", LegacySQLTypeName.INTEGER),
          com.google.cloud.bigquery.Field.of("mean", LegacySQLTypeName.FLOAT),
          com.google.cloud.bigquery.Field.of("stdev", LegacySQLTypeName.FLOAT),
          com.google.cloud.bigquery.Field.of("zeroes", LegacySQLTypeName.INTEGER),
          com.google.cloud.bigquery.Field.of("min", LegacySQLTypeName.FLOAT),
          com.google.cloud.bigquery.Field.of("max", LegacySQLTypeName.FLOAT),
          com.google.cloud.bigquery.Field.of("quantiles", LegacySQLTypeName.NUMERIC),
          com.google.cloud.bigquery.Field.of("unique", LegacySQLTypeName.INTEGER),
          com.google.cloud.bigquery.Field.of(
              "top_count",
              LegacySQLTypeName.RECORD,
              com.google.cloud.bigquery.Field.of("value", LegacySQLTypeName.STRING),
              com.google.cloud.bigquery.Field.of("count", LegacySQLTypeName.INTEGER)));

  private Schema histStatsSchema =
      Schema.of(
          com.google.cloud.bigquery.Field.of("feature", LegacySQLTypeName.STRING),
          com.google.cloud.bigquery.Field.of(
              "num_hist",
              LegacySQLTypeName.RECORD,
              com.google.cloud.bigquery.Field.of("count", LegacySQLTypeName.INTEGER),
              com.google.cloud.bigquery.Field.of("low_value", LegacySQLTypeName.FLOAT),
              com.google.cloud.bigquery.Field.of("high_value", LegacySQLTypeName.FLOAT)),
          com.google.cloud.bigquery.Field.of(
              "cat_hist",
              LegacySQLTypeName.RECORD,
              com.google.cloud.bigquery.Field.of("value", LegacySQLTypeName.STRING),
              com.google.cloud.bigquery.Field.of("count", LegacySQLTypeName.INTEGER)));

  @Test
  public void voidShouldConvertNumericStatsToFeatureNameStatistics()
      throws InvalidProtocolBufferException {
    FieldValueList numericFieldValueList =
        FieldValueList.of(
            Lists.newArrayList(
                FieldValue.of(Attribute.PRIMITIVE, "floats"),
                FieldValue.of(Attribute.PRIMITIVE, "20"),
                FieldValue.of(Attribute.PRIMITIVE, "20"),
                FieldValue.of(Attribute.PRIMITIVE, "0"),
                FieldValue.of(Attribute.PRIMITIVE, "1"),
                FieldValue.of(Attribute.PRIMITIVE, "6"),
                FieldValue.of(Attribute.PRIMITIVE, "0"),
                FieldValue.of(Attribute.PRIMITIVE, "-8.5"),
                FieldValue.of(Attribute.PRIMITIVE, "10.5"),
                FieldValue.of(
                    Attribute.REPEATED,
                    FieldValueList.of(
                        Lists.newArrayList(
                            FieldValue.of(Attribute.PRIMITIVE, "-8.5"),
                            FieldValue.of(Attribute.PRIMITIVE, "-7.5"),
                            FieldValue.of(Attribute.PRIMITIVE, "-5.5"),
                            FieldValue.of(Attribute.PRIMITIVE, "-3.5"),
                            FieldValue.of(Attribute.PRIMITIVE, "-1.5"),
                            FieldValue.of(Attribute.PRIMITIVE, "0.5"),
                            FieldValue.of(Attribute.PRIMITIVE, "2.5"),
                            FieldValue.of(Attribute.PRIMITIVE, "4.5"),
                            FieldValue.of(Attribute.PRIMITIVE, "6.5"),
                            FieldValue.of(Attribute.PRIMITIVE, "8.5"),
                            FieldValue.of(Attribute.PRIMITIVE, "10.5")))),
                FieldValue.of(Attribute.PRIMITIVE, null),
                FieldValue.of(Attribute.REPEATED, FieldValueList.of(Lists.newArrayList()))));

    FieldValueList numericHistFieldValueList =
        FieldValueList.of(
            Lists.newArrayList(
                FieldValue.of(Attribute.PRIMITIVE, "floats"),
                FieldValue.of(
                    Attribute.REPEATED,
                    FieldValueList.of(
                        Lists.newArrayList(
                            FieldValue.of(
                                Attribute.RECORD,
                                FieldValueList.of(
                                    Lists.newArrayList(
                                        FieldValue.of(Attribute.PRIMITIVE, "1"),
                                        FieldValue.of(Attribute.PRIMITIVE, "1"),
                                        FieldValue.of(Attribute.PRIMITIVE, "2")))),
                            FieldValue.of(
                                Attribute.RECORD,
                                FieldValueList.of(
                                    Lists.newArrayList(
                                        FieldValue.of(Attribute.PRIMITIVE, "2"),
                                        FieldValue.of(Attribute.PRIMITIVE, "2"),
                                        FieldValue.of(Attribute.PRIMITIVE, "3"))))))),
                FieldValue.of(Attribute.REPEATED, FieldValueList.of(Lists.newArrayList()))));

    FeatureSpec featureSpec =
        FeatureSpec.newBuilder().setName("floats").setValueType(Enum.DOUBLE).build();

    FeatureNameStatistics actual =
        toFeatureNameStatistics(
            featureSpec,
            basicStatsSchema,
            numericFieldValueList,
            histStatsSchema,
            numericHistFieldValueList);
    String expectedJson =
        "{\"type\":\"FLOAT\",\"numStats\":{\"commonStats\":{\"numNonMissing\":\"20\",\"minNumValues\":\"1\",\"maxNumValues\":\"1\",\"avgNumValues\":1,\"totNumValues\":\"20\"},\"mean\":1,\"stdDev\":6,\"min\":-8.5,\"median\":0.5,\"max\":10.5,\"histograms\":[{\"buckets\":[{\"lowValue\":-8.5,\"highValue\":-7.5,\"sampleCount\":2},{\"lowValue\":-7.5,\"highValue\":-5.5,\"sampleCount\":2},{\"lowValue\":-5.5,\"highValue\":-3.5,\"sampleCount\":2},{\"lowValue\":-3.5,\"highValue\":-1.5,\"sampleCount\":2},{\"lowValue\":-1.5,\"highValue\":0.5,\"sampleCount\":2},{\"lowValue\":0.5,\"highValue\":2.5,\"sampleCount\":2},{\"lowValue\":2.5,\"highValue\":4.5,\"sampleCount\":2},{\"lowValue\":4.5,\"highValue\":6.5,\"sampleCount\":2},{\"lowValue\":6.5,\"highValue\":8.5,\"sampleCount\":2},{\"lowValue\":8.5,\"highValue\":10.5,\"sampleCount\":2}],\"type\":\"QUANTILES\"},{\"buckets\":[{\"lowValue\":1,\"highValue\":2,\"sampleCount\":1},{\"lowValue\":2,\"highValue\":3,\"sampleCount\":2}]}]},\"path\":{\"step\":[\"floats\"]}}";
    FeatureNameStatistics.Builder expected = FeatureNameStatistics.newBuilder();
    JsonFormat.parser().merge(expectedJson, expected);
    assertThat(actual, equalTo(expected.build()));
  }

  @Test
  public void voidShouldConvertStringStatsToFeatureNameStatistics()
      throws InvalidProtocolBufferException {
    FieldValueList stringFieldValueList =
        FieldValueList.of(
            Lists.newArrayList(
                FieldValue.of(Attribute.PRIMITIVE, "strings"),
                FieldValue.of(Attribute.PRIMITIVE, "20"),
                FieldValue.of(Attribute.PRIMITIVE, "20"),
                FieldValue.of(Attribute.PRIMITIVE, "0"),
                FieldValue.of(Attribute.PRIMITIVE, null),
                FieldValue.of(Attribute.PRIMITIVE, null),
                FieldValue.of(Attribute.PRIMITIVE, null),
                FieldValue.of(Attribute.PRIMITIVE, null),
                FieldValue.of(Attribute.PRIMITIVE, null),
                FieldValue.of(Attribute.REPEATED, FieldValueList.of(Lists.newArrayList())),
                FieldValue.of(Attribute.PRIMITIVE, "2"),
                FieldValue.of(
                    Attribute.REPEATED,
                    FieldValueList.of(
                        Lists.newArrayList(
                            FieldValue.of(
                                Attribute.RECORD,
                                FieldValueList.of(
                                    Lists.newArrayList(
                                        FieldValue.of(Attribute.PRIMITIVE, "a"),
                                        FieldValue.of(Attribute.PRIMITIVE, "1")))),
                            FieldValue.of(
                                Attribute.RECORD,
                                FieldValueList.of(
                                    Lists.newArrayList(
                                        FieldValue.of(Attribute.PRIMITIVE, "b"),
                                        FieldValue.of(Attribute.PRIMITIVE, "2")))))))));

    FieldValueList stringHistFieldValueList =
        FieldValueList.of(
            Lists.newArrayList(
                FieldValue.of(Attribute.PRIMITIVE, "strings"),
                FieldValue.of(Attribute.REPEATED, FieldValueList.of(Lists.newArrayList())),
                FieldValue.of(
                    Attribute.REPEATED,
                    FieldValueList.of(
                        Lists.newArrayList(
                            FieldValue.of(
                                Attribute.RECORD,
                                FieldValueList.of(
                                    Lists.newArrayList(
                                        FieldValue.of(Attribute.PRIMITIVE, "a"),
                                        FieldValue.of(Attribute.PRIMITIVE, "1")))),
                            FieldValue.of(
                                Attribute.RECORD,
                                FieldValueList.of(
                                    Lists.newArrayList(
                                        FieldValue.of(Attribute.PRIMITIVE, "b"),
                                        FieldValue.of(Attribute.PRIMITIVE, "2")))))))));

    FeatureSpec featureSpec =
        FeatureSpec.newBuilder().setName("strings").setValueType(Enum.STRING).build();

    FeatureNameStatistics actual =
        toFeatureNameStatistics(
            featureSpec,
            basicStatsSchema,
            stringFieldValueList,
            histStatsSchema,
            stringHistFieldValueList);
    String expectedJson =
        "{\"type\":\"STRING\",\"stringStats\":{\"commonStats\":{\"numNonMissing\":\"20\",\"minNumValues\":\"1\",\"maxNumValues\":\"1\",\"avgNumValues\":1,\"totNumValues\":\"20\"},\"unique\":\"2\",\"topValues\":[{\"value\":\"a\",\"frequency\":1},{\"value\":\"b\",\"frequency\":2}],\"rankHistogram\":{\"buckets\":[{\"label\":\"a\",\"sampleCount\":1},{\"label\":\"b\",\"sampleCount\":2}]}},\"path\":{\"step\":[\"strings\"]}}";
    FeatureNameStatistics.Builder expected = FeatureNameStatistics.newBuilder();
    JsonFormat.parser().merge(expectedJson, expected);
    assertThat(actual, equalTo(expected.build()));
  }
}
