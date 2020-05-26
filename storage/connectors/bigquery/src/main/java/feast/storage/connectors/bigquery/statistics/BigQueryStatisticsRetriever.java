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
import feast.proto.core.FeatureSetProto.FeatureSpec;
import feast.proto.core.StoreProto.Store.BigQueryConfig;
import feast.storage.api.statistics.FeatureSetStatistics;
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

  static Builder newBuilder() {
    return new AutoValue_BigQueryStatisticsRetriever.Builder();
  }

  @AutoValue.Builder
  abstract static class Builder {

    public abstract Builder setProjectId(String projectId);

    public abstract Builder setDatasetId(String datasetId);

    public abstract Builder setBigquery(BigQuery bigquery);

    public abstract BigQueryStatisticsRetriever build();
  }

  @Override
  public FeatureSetStatistics getFeatureStatistics(
      FeatureSetSpec featureSetSpec, List<String> features, String dataset) {
    FeatureSetStatisticsQueryInfo featureSetStatisticsQueryInfo =
        new FeatureSetStatisticsQueryInfo(
            featureSetSpec.getProject(), featureSetSpec.getName(), dataset);
    return getFeatureSetStatistics(featureSetSpec, features, featureSetStatisticsQueryInfo);
  }

  @Override
  public FeatureSetStatistics getFeatureStatistics(
      FeatureSetSpec featureSetSpec, List<String> features, Timestamp date) {
    FeatureSetStatisticsQueryInfo featureSetStatisticsQueryInfo =
        new FeatureSetStatisticsQueryInfo(
            featureSetSpec.getProject(), featureSetSpec.getName(), date);
    return getFeatureSetStatistics(featureSetSpec, features, featureSetStatisticsQueryInfo);
  }

  private FeatureSetStatistics getFeatureSetStatistics(
      FeatureSetSpec featureSetSpec,
      List<String> features,
      FeatureSetStatisticsQueryInfo featureSetStatisticsQueryInfo) {
    List<FeatureSpec> featuresList = featureSetSpec.getFeaturesList();

    FeatureSetSpec.Builder featureSetSpecBuilder = featureSetSpec.toBuilder().clearFeatures();
    for (FeatureSpec featureSpec : featuresList) {
      if (features.contains(featureSpec.getName())) {
        featureSetStatisticsQueryInfo.addFeature(featureSpec);
        featureSetSpecBuilder.addFeatures(featureSpec);
      }
    }
    featureSetSpec = featureSetSpecBuilder.build();

    try {
      // Generate SQL for and retrieve non-histogram statistics
      String getFeatureSetStatsQuery =
          StatsQueryTemplater.createGetFeatureSetStatsQuery(
              featureSetStatisticsQueryInfo, projectId(), datasetId());
      QueryJobConfiguration queryJobConfiguration =
          QueryJobConfiguration.newBuilder(getFeatureSetStatsQuery).build();
      TableResult basicStats = bigquery().query(queryJobConfiguration);

      // Generate SQL for and retrieve histogram statistics
      String getFeatureSetHistQuery =
          StatsQueryTemplater.createGetFeatureSetHistQuery(
              featureSetStatisticsQueryInfo, projectId(), datasetId());
      queryJobConfiguration = QueryJobConfiguration.newBuilder(getFeatureSetHistQuery).build();
      TableResult hist = bigquery().query(queryJobConfiguration);

      // Convert to map of feature_name:row containing the statistics
      Map<String, FieldValueList> basicStatsValues = getTableResultByFeatureName(basicStats);
      Map<String, FieldValueList> histValues = getTableResultByFeatureName(hist);

      int totalCountIndex = basicStats.getSchema().getFields().getIndex("total_count");
      String ref = features.get(0);
      FeatureSetStatistics.Builder featureSetStatisticsBuilder =
          FeatureSetStatistics.newBuilder()
              .setNumExamples(basicStatsValues.get(ref).get(totalCountIndex).getLongValue());

      // Convert BQ rows to FeatureNameStatistics
      for (FeatureSpec featureSpec : featureSetSpec.getFeaturesList()) {
        FeatureNameStatistics featureNameStatistics =
            StatsQueryResult.create()
                .withBasicStatsResults(
                    basicStats.getSchema(), basicStatsValues.get(featureSpec.getName()))
                .withHistResults(hist.getSchema(), histValues.get(featureSpec.getName()))
                .toFeatureNameStatistics(featureSpec.getValueType());
        featureSetStatisticsBuilder.addFeatureNameStatistics(featureNameStatistics);
      }
      return featureSetStatisticsBuilder.build();
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(
          String.format(
              "Unable to retrieve statistics from BigQuery for Feature set %s, features %s",
              featureSetSpec.getName(), features),
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
}
