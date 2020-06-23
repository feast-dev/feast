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
package feast.storage.connectors.bigquery.statistics;

import com.google.auto.value.AutoValue;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.Schema;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.tensorflow.metadata.v0.*;
import org.tensorflow.metadata.v0.Histogram.Bucket;
import org.tensorflow.metadata.v0.Histogram.HistogramType;
import org.tensorflow.metadata.v0.StringStatistics.FreqAndValue;

@AutoValue
public abstract class StatsQueryResult {

  // Schema of the table returned by the basic stats retrieval query
  @Nullable
  abstract Schema basicStatsSchema();

  // Table values returned by the basic stats retrieval query
  @Nullable
  abstract FieldValueList basicStatsFieldValues();

  // Schema of the table returned by the histogram retrieval query
  @Nullable
  abstract Schema histSchema();

  // Table values returned by the histogram retrieval query
  @Nullable
  abstract FieldValueList histFieldValues();

  public static StatsQueryResult create() {
    return StatsQueryResult.newBuilder().build();
  }

  private static StatsQueryResult.Builder newBuilder() {
    return new AutoValue_StatsQueryResult.Builder();
  }

  abstract Builder toBuilder();

  /**
   * Add basic stats query results to the StatsQueryResult.
   *
   * @param basicStatsSchema BigQuery {@link Schema} of the retrieved statistics row for the
   *     non-histogram statistics. Used to retrieve the column names corresponding to each value in
   *     the row.
   * @param basicStatsFieldValues BigQuery {@link FieldValueList} containing a single row of
   *     non-histogram statistics retrieved from BigQuery
   * @return {@link StatsQueryResult}
   */
  public StatsQueryResult withBasicStatsResults(
      Schema basicStatsSchema, FieldValueList basicStatsFieldValues) {
    return toBuilder()
        .setBasicStatsSchema(basicStatsSchema)
        .setBasicStatsFieldValues(basicStatsFieldValues)
        .build();
  }

  /**
   * Add histogram stats query results to the StatsQueryResult.
   *
   * @param histSchema BigQuery {@link Schema} of the retrieved statistics row for the histogram
   *     statistics. Used to retrieve the column names corresponding to each value in the row.
   * @param histFieldValues BigQuery {@link FieldValueList} containing a single row of histogram
   *     statistics retrieved from BigQuery
   * @return {@link StatsQueryResult}
   */
  public StatsQueryResult withHistResults(Schema histSchema, FieldValueList histFieldValues) {
    return toBuilder().setHistSchema(histSchema).setHistFieldValues(histFieldValues).build();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setBasicStatsSchema(Schema basicStatsSchema);

    abstract Builder setBasicStatsFieldValues(FieldValueList basicStatsFieldValues);

    abstract Builder setHistSchema(Schema histSchema);

    abstract Builder setHistFieldValues(FieldValueList histFieldValues);

    public abstract StatsQueryResult build();
  }

  /**
   * Convert BQ-retrieved statistics to the corresponding TFDV {@link FeatureNameStatistics}
   * specific to the feature type.
   *
   * @param featureInfo {@link FeatureStatisticsQueryInfo} containing information about the feature
   * @return {@link FeatureNameStatistics}
   */
  public FeatureNameStatistics toFeatureNameStatistics(FeatureStatisticsQueryInfo featureInfo) {
    Map<String, FieldValue> valuesMap = new HashMap<>();

    // Convert the table values to a map of field name : table value for easy retrieval
    FieldList basicStatsfields = basicStatsSchema().getFields();
    for (int i = 0; i < basicStatsSchema().getFields().size(); i++) {
      valuesMap.put(basicStatsfields.get(i).getName(), basicStatsFieldValues().get(i));
    }

    FieldList histFields = histSchema().getFields();
    for (int i = 0; i < histSchema().getFields().size(); i++) {
      valuesMap.put(histFields.get(i).getName(), histFieldValues().get(i));
    }

    FeatureNameStatistics.Builder featureNameStatisticsBuilder =
        FeatureNameStatistics.newBuilder()
            .setPath(Path.newBuilder().addStep(valuesMap.get("feature_name").getStringValue()))
            .setType(FeatureNameStatistics.Type.valueOf(featureInfo.getValueType()));

    switch (StatsType.Enum.valueOf(featureInfo.getStatsType())) {
      case NUMERIC:
        NumericStatistics numStats = getNumericStatistics(valuesMap);
        featureNameStatisticsBuilder.setNumStats(numStats);
        break;
      case CATEGORICAL:
        StringStatistics stringStats = getStringStatistics(valuesMap);
        featureNameStatisticsBuilder.setStringStats(stringStats);
        break;
      case BYTES:
        BytesStatistics bytesStats = getBytesStatistics(valuesMap);
        featureNameStatisticsBuilder.setBytesStats(bytesStats);
        break;
      case LIST:
        StructStatistics structStats = getStructStatistics(valuesMap);
        featureNameStatisticsBuilder.setStructStats(structStats);
        break;
      default:
        throw new IllegalArgumentException(
            "Invalid feature type provided. Only statistics for numeric, bytes, string, boolean and list features are supported.");
    }
    return featureNameStatisticsBuilder.build();
  }

  private BytesStatistics getBytesStatistics(Map<String, FieldValue> valuesMap) {
    if (valuesMap.get("total_count").getLongValue() == 0) {
      return BytesStatistics.getDefaultInstance();
    }

    return BytesStatistics.newBuilder()
        .setCommonStats(
            CommonStatistics.newBuilder()
                .setNumMissing(valuesMap.get("missing_count").getLongValue())
                .setNumNonMissing(valuesMap.get("feature_count").getLongValue())
                .setMinNumValues(1)
                .setMaxNumValues(1)
                .setAvgNumValues(1)
                .setTotNumValues(valuesMap.get("feature_count").getLongValue()))
        .setUnique(valuesMap.get("unique").getLongValue())
        .setMaxNumBytes((float) valuesMap.get("max").getDoubleValue())
        .setMinNumBytes((float) valuesMap.get("min").getDoubleValue())
        .setAvgNumBytes((float) valuesMap.get("mean").getDoubleValue())
        .build();
  }

  private StringStatistics getStringStatistics(Map<String, FieldValue> valuesMap) {
    if (valuesMap.get("total_count").getLongValue() == 0) {
      return StringStatistics.getDefaultInstance();
    }

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

    List<FreqAndValue> topCount =
        rankHistogram.getBucketsList().stream()
            .sorted(
                (a, b) ->
                    ComparisonChain.start()
                        .compare(
                            a.getSampleCount(), b.getSampleCount(), Ordering.natural().reverse())
                        .result())
            .limit(5)
            .map(
                bucket ->
                    FreqAndValue.newBuilder()
                        .setValue(bucket.getLabel())
                        .setFrequency(bucket.getSampleCount())
                        .build())
            .collect(Collectors.toList());

    return StringStatistics.newBuilder()
        .setUnique(valuesMap.get("unique").getLongValue())
        .setAvgLength((long) valuesMap.get("mean").getDoubleValue())
        .setCommonStats(
            CommonStatistics.newBuilder()
                .setNumMissing(valuesMap.get("missing_count").getLongValue())
                .setNumNonMissing(valuesMap.get("feature_count").getLongValue())
                .setMinNumValues(1)
                .setMaxNumValues(1)
                .setAvgNumValues(1)
                .setTotNumValues(valuesMap.get("feature_count").getLongValue()))
        .setRankHistogram(rankHistogram)
        .addAllTopValues(topCount)
        .build();
  }

  private NumericStatistics getNumericStatistics(Map<String, FieldValue> valuesMap) {
    if (valuesMap.get("total_count").getLongValue() == 0) {
      return NumericStatistics.getDefaultInstance();
    }

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
        .addHistograms(histBuilder)
        .addHistograms(quantilesBuilder)
        .build();
  }

  private StructStatistics getStructStatistics(Map<String, FieldValue> valuesMap) {
    if (valuesMap.get("total_count").getLongValue() == 0) {
      return StructStatistics.getDefaultInstance();
    }

    return StructStatistics.newBuilder()
        .setCommonStats(
            CommonStatistics.newBuilder()
                .setNumMissing(valuesMap.get("missing_count").getLongValue())
                .setNumNonMissing(valuesMap.get("feature_count").getLongValue())
                .setMinNumValues(valuesMap.get("min").getLongValue())
                .setMaxNumValues(valuesMap.get("max").getLongValue())
                .setAvgNumValues(valuesMap.get("mean").getLongValue())
                .setTotNumValues(
                    valuesMap.get("feature_count").getLongValue()
                        * valuesMap.get("mean").getLongValue()))
        .build();
  }
}
