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
package feast.storage.connectors.bigquery.write;

import com.google.auto.value.AutoValue;
import com.google.cloud.bigquery.*;
import com.google.common.collect.ImmutableMap;
import feast.core.FeatureSetProto;
import feast.core.StoreProto.Store.BigQueryConfig;
import feast.storage.api.write.FeatureSink;
import feast.storage.api.write.WriteResult;
import feast.storage.connectors.bigquery.common.TypeUtil;
import feast.storage.connectors.bigquery.write.AutoValue_BigQueryFeatureSink.Builder;
import feast.types.FeatureRowProto;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;

@AutoValue
public abstract class BigQueryFeatureSink implements FeatureSink {
  private static final Logger log = org.slf4j.LoggerFactory.getLogger(BigQueryFeatureSink.class);

  // Column description for reserved fields
  public static final String BIGQUERY_EVENT_TIMESTAMP_FIELD_DESCRIPTION =
      "Event time for the FeatureRow";
  public static final String BIGQUERY_CREATED_TIMESTAMP_FIELD_DESCRIPTION =
      "Processing time of the FeatureRow ingestion in Feast\"";
  public static final String BIGQUERY_JOB_ID_FIELD_DESCRIPTION =
      "Feast import job ID for the FeatureRow";

  public abstract String getProjectId();

  public abstract String getDatasetId();

  public abstract BigQuery getBigQuery();

  /**
   * Initialize a {@link BigQueryFeatureSink.Builder} from a {@link BigQueryConfig}. This method
   * initializes a {@link BigQuery} client with default options. Use the builder method to inject
   * your own client.
   *
   * @param config {@link BigQueryConfig}
   * @return {@link BigQueryFeatureSink.Builder}
   */
  public static BigQueryFeatureSink fromConfig(BigQueryConfig config) {
    return builder()
        .setDatasetId(config.getDatasetId())
        .setProjectId(config.getProjectId())
        .setBigQuery(BigQueryOptions.getDefaultInstance().getService())
        .build();
  }

  public static Builder builder() {
    return new AutoValue_BigQueryFeatureSink.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setProjectId(String projectId);

    public abstract Builder setDatasetId(String datasetId);

    public abstract Builder setBigQuery(BigQuery bigQuery);

    public abstract BigQueryFeatureSink build();
  }

  /** @param featureSet Feature set to be written */
  @Override
  public void prepareWrite(FeatureSetProto.FeatureSet featureSet) {
    BigQuery bigquery = getBigQuery();
    FeatureSetProto.FeatureSetSpec featureSetSpec = featureSet.getSpec();

    DatasetId datasetId = DatasetId.of(getProjectId(), getDatasetId());
    if (bigquery.getDataset(datasetId) == null) {
      log.info(
          "Creating dataset '{}' in project '{}'", datasetId.getDataset(), datasetId.getProject());
      bigquery.create(DatasetInfo.of(datasetId));
    }
    String tableName =
        String.format(
                "%s_%s_v%d",
                featureSetSpec.getProject(), featureSetSpec.getName(), featureSetSpec.getVersion())
            .replaceAll("-", "_");
    TableId tableId = TableId.of(datasetId.getProject(), datasetId.getDataset(), tableName);

    // Return if there is an existing table
    Table table = bigquery.getTable(tableId);
    if (table != null) {
      log.info(
          "Writing to existing BigQuery table '{}:{}.{}'",
          getProjectId(),
          datasetId.getDataset(),
          tableName);
      return;
    }

    log.info(
        "Creating table '{}' in dataset '{}' in project '{}'",
        tableId.getTable(),
        datasetId.getDataset(),
        datasetId.getProject());
    TableDefinition tableDefinition = createBigQueryTableDefinition(featureSet.getSpec());
    TableInfo tableInfo = TableInfo.of(tableId, tableDefinition);
    bigquery.create(tableInfo);
  }

  @Override
  public PTransform<PCollection<FeatureRowProto.FeatureRow>, WriteResult> write() {
    return new BigQueryWrite(DatasetId.of(getProjectId(), getDatasetId()));
  }

  private TableDefinition createBigQueryTableDefinition(FeatureSetProto.FeatureSetSpec spec) {
    List<Field> fields = new ArrayList<>();
    log.info("Table will have the following fields:");

    for (FeatureSetProto.EntitySpec entitySpec : spec.getEntitiesList()) {
      Field.Builder builder =
          Field.newBuilder(
              entitySpec.getName(), TypeUtil.toStandardSqlType(entitySpec.getValueType()));
      if (entitySpec.getValueType().name().toLowerCase().endsWith("_list")) {
        builder.setMode(Field.Mode.REPEATED);
      }
      Field field = builder.build();
      log.info("- {}", field.toString());
      fields.add(field);
    }
    for (FeatureSetProto.FeatureSpec featureSpec : spec.getFeaturesList()) {
      Field.Builder builder =
          Field.newBuilder(
              featureSpec.getName(), TypeUtil.toStandardSqlType(featureSpec.getValueType()));
      if (featureSpec.getValueType().name().toLowerCase().endsWith("_list")) {
        builder.setMode(Field.Mode.REPEATED);
      }
      Field field = builder.build();
      log.info("- {}", field.toString());
      fields.add(field);
    }

    // Refer to protos/feast/core/Store.proto for reserved fields in BigQuery.
    Map<String, Pair<StandardSQLTypeName, String>>
        reservedFieldNameToPairOfStandardSQLTypeAndDescription =
            ImmutableMap.of(
                "event_timestamp",
                Pair.of(StandardSQLTypeName.TIMESTAMP, BIGQUERY_EVENT_TIMESTAMP_FIELD_DESCRIPTION),
                "created_timestamp",
                Pair.of(
                    StandardSQLTypeName.TIMESTAMP, BIGQUERY_CREATED_TIMESTAMP_FIELD_DESCRIPTION),
                "job_id",
                Pair.of(StandardSQLTypeName.STRING, BIGQUERY_JOB_ID_FIELD_DESCRIPTION));
    for (Map.Entry<String, Pair<StandardSQLTypeName, String>> entry :
        reservedFieldNameToPairOfStandardSQLTypeAndDescription.entrySet()) {
      Field field =
          Field.newBuilder(entry.getKey(), entry.getValue().getLeft())
              .setDescription(entry.getValue().getRight())
              .build();
      log.info("- {}", field.toString());
      fields.add(field);
    }

    TimePartitioning timePartitioning =
        TimePartitioning.newBuilder(TimePartitioning.Type.DAY).setField("event_timestamp").build();
    log.info("Table partitioning: " + timePartitioning.toString());

    return StandardTableDefinition.newBuilder()
        .setTimePartitioning(timePartitioning)
        .setSchema(Schema.of(fields))
        .build();
  }
}
