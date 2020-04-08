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

import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Schema;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.types.ValueProto.ValueType;
import feast.types.ValueProto.ValueType.Enum;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.tensorflow.metadata.v0.*;
import org.tensorflow.metadata.v0.FeatureNameStatistics.Builder;
import org.tensorflow.metadata.v0.FeatureNameStatistics.Type;
import org.tensorflow.metadata.v0.Histogram.Bucket;
import org.tensorflow.metadata.v0.Histogram.HistogramType;
import org.tensorflow.metadata.v0.StringStatistics.FreqAndValue;

public class StatsUtil {
  private static final Map<ValueType.Enum, Type> TFDV_TYPE_MAP = new HashMap<>();

  static {
    TFDV_TYPE_MAP.put(Enum.INT64, Type.INT);
    TFDV_TYPE_MAP.put(Enum.INT32, Type.INT);
    TFDV_TYPE_MAP.put(Enum.BOOL, Type.INT);
    TFDV_TYPE_MAP.put(Enum.FLOAT, Type.FLOAT);
    TFDV_TYPE_MAP.put(Enum.DOUBLE, Type.FLOAT);
    TFDV_TYPE_MAP.put(Enum.STRING, Type.STRING);
    TFDV_TYPE_MAP.put(Enum.BYTES, Type.BYTES);
    TFDV_TYPE_MAP.put(Enum.BYTES_LIST, Type.STRUCT);
    TFDV_TYPE_MAP.put(Enum.STRING_LIST, Type.STRUCT);
    TFDV_TYPE_MAP.put(Enum.INT32_LIST, Type.STRUCT);
    TFDV_TYPE_MAP.put(Enum.INT64_LIST, Type.STRUCT);
    TFDV_TYPE_MAP.put(Enum.BOOL_LIST, Type.STRUCT);
    TFDV_TYPE_MAP.put(Enum.FLOAT_LIST, Type.STRUCT);
    TFDV_TYPE_MAP.put(Enum.DOUBLE_LIST, Type.STRUCT);
  }

  public static FeatureNameStatistics toFeatureNameStatistics(
      FeatureSpec featureSpec,
      Schema basicStatsSchema,
      FieldValueList basicStatsValues,
      Schema histSchema,
      FieldValueList histValues) {
    Map<String, FieldValue> valuesMap = new HashMap<>();

    FieldList basicStatsfields = basicStatsSchema.getFields();
    for (int i = 0; i < basicStatsSchema.getFields().size(); i++) {
      valuesMap.put(basicStatsfields.get(i).getName(), basicStatsValues.get(i));
    }

    FieldList histFields = histSchema.getFields();
    for (int i = 0; i < histSchema.getFields().size(); i++) {
      valuesMap.put(histFields.get(i).getName(), histValues.get(i));
    }

    Builder featureNameStatisticsBuilder =
        FeatureNameStatistics.newBuilder()
            .setPath(Path.newBuilder().addStep(valuesMap.get("feature_name").getStringValue()))
            .setType(TFDV_TYPE_MAP.get(featureSpec.getValueType()));

    switch (featureSpec.getValueType()) {
      case FLOAT:
      case BOOL:
      case DOUBLE:
      case INT32:
      case INT64:
        NumericStatistics numStats = getNumericStatistics(valuesMap);
        featureNameStatisticsBuilder.setNumStats(numStats);
        break;
      case STRING:
        StringStatistics stringStats = getStringStatistics(valuesMap);
        featureNameStatisticsBuilder.setStringStats(stringStats);
        break;
      case BYTES:
        BytesStatistics bytesStats = getBytesStatistics(valuesMap);
        featureNameStatisticsBuilder.setBytesStats(bytesStats);
        break;
      case BYTES_LIST:
      case BOOL_LIST:
      case FLOAT_LIST:
      case INT32_LIST:
      case INT64_LIST:
      case DOUBLE_LIST:
      case STRING_LIST:
        StructStatistics structStats = getStructStatistics(valuesMap);
        featureNameStatisticsBuilder.setStructStats(structStats);
        break;
      default:
        throw new IllegalArgumentException(
            "Invalid feature type provided. Only statistics for numeric, bytes, string, boolean and list features are supported.");
    }
    return featureNameStatisticsBuilder.build();
  }

  private static BytesStatistics getBytesStatistics(Map<String, FieldValue> valuesMap) {
    return BytesStatistics.newBuilder()
        .setCommonStats(
            CommonStatistics.newBuilder()
                .setNumMissing(valuesMap.get("missing_count").getLongValue())
                .setNumNonMissing(valuesMap.get("feature_count").getLongValue())
                .setMinNumValues(1)
                .setMaxNumValues(1)
                .setAvgNumValues(1)
                .setTotNumValues(valuesMap.get("total_count").getLongValue()))
        .setUnique(valuesMap.get("unique").getLongValue())
        .setMaxNumBytes((float) valuesMap.get("max").getDoubleValue())
        .setMinNumBytes((float) valuesMap.get("min").getDoubleValue())
        .setAvgNumBytes((float) valuesMap.get("mean").getDoubleValue())
        .build();
  }

  private static StringStatistics getStringStatistics(Map<String, FieldValue> valuesMap) {
    List<FreqAndValue> topCount =
        valuesMap.get("top_count").getRepeatedValue().stream()
            .map(
                tc -> {
                  FieldValueList recordValue = tc.getRecordValue();
                  return FreqAndValue.newBuilder()
                      .setValue(recordValue.get(0).getStringValue())
                      .setFrequency(recordValue.get(1).getLongValue())
                      .build();
                })
            .collect(Collectors.toList());

    RankHistogram.Builder rankHistogram = RankHistogram.newBuilder();
    valuesMap
        .get("cat_hist")
        .getRepeatedValue()
        .forEach(
            v -> {
              FieldValueList recordValue = v.getRecordValue();
              rankHistogram.addBuckets(
                  RankHistogram.Bucket.newBuilder()
                      .setLabel(recordValue.get(0).getStringValue())
                      .setSampleCount(recordValue.get(1).getLongValue()));
            });

    return StringStatistics.newBuilder()
        .setUnique(valuesMap.get("unique").getLongValue())
        .setCommonStats(
            CommonStatistics.newBuilder()
                .setNumMissing(valuesMap.get("missing_count").getLongValue())
                .setNumNonMissing(valuesMap.get("feature_count").getLongValue())
                .setMinNumValues(1)
                .setMaxNumValues(1)
                .setAvgNumValues(1)
                .setTotNumValues(valuesMap.get("total_count").getLongValue()))
        .setRankHistogram(rankHistogram)
        .addAllTopValues(topCount)
        .build();
  }

  private static NumericStatistics getNumericStatistics(Map<String, FieldValue> valuesMap) {
    // Build quantiles
    long quantileCount = valuesMap.get("feature_count").getLongValue() / 10;
    Histogram.Builder quantilesBuilder = Histogram.newBuilder().setType(HistogramType.QUANTILES);

    List<FieldValue> quantilesRaw = valuesMap.get("quantiles").getRepeatedValue();
    for (int i = 0; i < quantilesRaw.size() - 1; i++) {
      quantilesBuilder.addBuckets(
          Bucket.newBuilder()
              .setLowValue(quantilesRaw.get(i).getDoubleValue())
              .setHighValue(quantilesRaw.get(i + 1).getDoubleValue())
              .setSampleCount(quantileCount));
    }
    // Build histogram
    Histogram.Builder histBuilder = Histogram.newBuilder().setType(HistogramType.STANDARD);

    // Order of histogram records is defined in the query hist_stats.sql:L35
    valuesMap
        .get("num_hist")
        .getRepeatedValue()
        .forEach(
            v -> {
              FieldValueList recordValue = v.getRecordValue();
              histBuilder.addBuckets(
                  Bucket.newBuilder()
                      .setHighValue(recordValue.get(2).getDoubleValue())
                      .setLowValue(recordValue.get(1).getDoubleValue())
                      .setSampleCount(recordValue.get(0).getLongValue()));
            });

    return NumericStatistics.newBuilder()
        .setMax(valuesMap.get("max").getDoubleValue())
        .setMin(valuesMap.get("min").getDoubleValue())
        .setMedian(quantilesRaw.get(5).getDoubleValue())
        .setNumZeros(valuesMap.get("zeroes").getLongValue())
        .setStdDev(valuesMap.get("stdev").getDoubleValue())
        .setMean(valuesMap.get("mean").getDoubleValue())
        .setCommonStats(
            CommonStatistics.newBuilder()
                .setNumMissing(valuesMap.get("missing_count").getLongValue())
                .setNumNonMissing(valuesMap.get("feature_count").getLongValue())
                .setMinNumValues(1)
                .setMaxNumValues(1)
                .setAvgNumValues(1)
                .setTotNumValues(valuesMap.get("feature_count").getLongValue()))
        .addHistograms(quantilesBuilder)
        .addHistograms(histBuilder)
        .build();
  }

  private static StructStatistics getStructStatistics(Map<String, FieldValue> valuesMap) {
    return StructStatistics.newBuilder()
        .setCommonStats(
            CommonStatistics.newBuilder()
                .setNumMissing(valuesMap.get("missing_count").getLongValue())
                .setNumNonMissing(valuesMap.get("feature_count").getLongValue())
                .setMinNumValues(valuesMap.get("min").getLongValue())
                .setMaxNumValues(valuesMap.get("max").getLongValue())
                .setAvgNumValues(valuesMap.get("mean").getLongValue())
                .setTotNumValues(valuesMap.get("total_count").getLongValue()))
        .build();
  }
}
