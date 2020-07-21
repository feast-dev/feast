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
import com.google.common.collect.Streams;
import com.google.protobuf.Timestamp;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.StoreProto.Store.BigQueryConfig;
import feast.storage.api.statistics.FeatureStatistics;
import feast.storage.api.statistics.StatisticsRetriever;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.tensorflow.metadata.v0.FeatureNameStatistics;

@AutoValue
public abstract class BigQueryStatisticsRetriever implements StatisticsRetriever {

  public abstract String projectId();

  public abstract String datasetId();

  public abstract BigQuery bigquery();

  public static BigQueryStatisticsRetriever create(BigQueryConfig config) {
    BigQuery bigquery =
        BigQueryOptions.getDefaultInstance()
            .toBuilder()
            .setProjectId(config.getProjectId())
            .build()
            .getService();
    return newBuilder()
        .setBigquery(bigquery)
        .setDatasetId(config.getDatasetId())
        .setProjectId(config.getProjectId())
        .build();
  }

  public static Builder newBuilder() {
    return new AutoValue_BigQueryStatisticsRetriever.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setProjectId(String projectId);

    public abstract Builder setDatasetId(String datasetId);

    public abstract Builder setBigquery(BigQuery bigquery);

    public abstract BigQueryStatisticsRetriever build();
  }

  @Override
  public FeatureStatistics getFeatureStatistics(
      FeatureSetSpec featureSetSpec, List<String> features, String ingestionId) {
    StatsDataset queryDataset = buildStatsDataset(featureSetSpec);
    queryDataset.subsetByIngestionId(ingestionId);
    List<FeatureStatisticsQueryInfo> featureStatisticsQueryInfos =
        featureSetSpec.getFeaturesList().stream()
            .filter(f -> features.contains(f.getName()))
            .map(FeatureStatisticsQueryInfo::fromProto)
            .collect(Collectors.toList());
    return getFeatureStatistics(featureStatisticsQueryInfos, queryDataset);
  }

  @Override
  public FeatureStatistics getFeatureStatistics(
      FeatureSetSpec featureSetSpec, List<String> features, Timestamp date) {
    StatsDataset queryDataset = buildStatsDataset(featureSetSpec);
    queryDataset.subsetByDate(date);
    List<FeatureStatisticsQueryInfo> featureStatisticsQueryInfos =
        featureSetSpec.getFeaturesList().stream()
            .filter(f -> features.contains(f.getName()))
            .map(FeatureStatisticsQueryInfo::fromProto)
            .collect(Collectors.toList());
    return getFeatureStatistics(featureStatisticsQueryInfos, queryDataset);
  }

  public FeatureStatistics getFeatureStatistics(
      List<FeatureStatisticsQueryInfo> features, StatsDataset statsDataset)
      throws RuntimeException {
    try {
      // Generate SQL for and retrieve non-histogram statistics
      String getFeatureSetStatsQuery =
          StatsQueryTemplater.createGetFeaturesStatsQuery(features, statsDataset);
      QueryJobConfiguration queryJobConfiguration =
          QueryJobConfiguration.newBuilder(getFeatureSetStatsQuery).build();
      TableResult basicStats = bigquery().query(queryJobConfiguration);

      // Generate SQL for and retrieve histogram statistics
      String getFeatureSetHistQuery =
          StatsQueryTemplater.createGetFeaturesHistQuery(features, statsDataset);
      queryJobConfiguration = QueryJobConfiguration.newBuilder(getFeatureSetHistQuery).build();
      TableResult hist = bigquery().query(queryJobConfiguration);

      // Convert to map of feature_name:row containing the statistics
      Map<String, FieldValueList> basicStatsValues = getTableResultByFeatureName(basicStats);
      Map<String, FieldValueList> histValues = getTableResultByFeatureName(hist);

      int totalCountIndex = basicStats.getSchema().getFields().getIndex("total_count");
      String ref = features.get(0).getName();
      FeatureStatistics.Builder featureSetStatisticsBuilder =
          FeatureStatistics.newBuilder()
              .setNumExamples(basicStatsValues.get(ref).get(totalCountIndex).getLongValue());

      // Convert BQ rows to FeatureNameStatistics
      for (FeatureStatisticsQueryInfo featureInfo : features) {
        FeatureNameStatistics featureNameStatistics =
            StatsQueryResult.create()
                .withBasicStatsResults(
                    basicStats.getSchema(), basicStatsValues.get(featureInfo.getName()))
                .withHistResults(hist.getSchema(), histValues.get(featureInfo.getName()))
                .toFeatureNameStatistics(featureInfo);
        featureSetStatisticsBuilder.addFeatureNameStatistics(featureNameStatistics);
      }
      return featureSetStatisticsBuilder.build();
    } catch (IOException | InterruptedException e) {
      String featuresList =
          features.stream()
              .map(FeatureStatisticsQueryInfo::getName)
              .collect(Collectors.joining(","));
      throw new RuntimeException(
          String.format(
              "Unable to retrieve statistics from BigQuery for features %s", featuresList),
          e);
    }
  }

  private Map<String, FieldValueList> getTableResultByFeatureName(TableResult basicStats) {
    return Streams.stream(basicStats.getValues())
        .collect(
            Collectors.toMap(
                fieldValueList -> fieldValueList.get(0).getStringValue(),
                fieldValueList -> fieldValueList));
  }

  private StatsDataset buildStatsDataset(FeatureSetSpec featureSetSpec) {
    String featureSetTableName =
        String.format("%s_%s", featureSetSpec.getProject(), featureSetSpec.getName());
    return new StatsDataset(projectId(), datasetId(), featureSetTableName);
  }
}
