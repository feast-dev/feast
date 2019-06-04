/*
 * Copyright 2018 The Feast Authors
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
 *
 */
package feast.core.training;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.JobOption;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.JobException;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.common.base.Strings;
import com.google.protobuf.Timestamp;
import feast.core.DatasetServiceProto.DatasetInfo;
import feast.core.DatasetServiceProto.FeatureSet;
import feast.core.exception.TrainingDatasetCreationException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BigQueryTraningDatasetCreator {

  private final BigQueryDatasetTemplater templater;
  private final DateTimeFormatter formatter;
  private final String projectId;
  private final String datasetPrefix;
  private transient BigQuery bigQuery;


  public BigQueryTraningDatasetCreator(
      BigQueryDatasetTemplater templater,
      String projectId,
      String datasetPrefix) {
    this(templater, projectId, datasetPrefix,
        BigQueryOptions.newBuilder().setProjectId(projectId).build().getService());
  }

  public BigQueryTraningDatasetCreator(
      BigQueryDatasetTemplater templater,
      String projectId,
      String datasetPrefix,
      BigQuery bigQuery) {
    this.templater = templater;
    this.formatter = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneId.of("UTC"));
    this.projectId = projectId;
    this.datasetPrefix = datasetPrefix;
    this.bigQuery = bigQuery;
  }

  /**
   * Create dataset for a feature set
   *
   * @param featureSet feature set for which the training dataset should be created
   * @param startDate starting date of the training dataset (inclusive)
   * @param endDate end date of the training dataset (inclusive)
   * @param limit maximum number of row should be created.
   * @param namePrefix prefix for dataset name
   * @return dataset info associated with the created training dataset
   */
  public DatasetInfo createDataset(
      FeatureSet featureSet,
      Timestamp startDate,
      Timestamp endDate,
      long limit,
      String namePrefix) {
    try {
      String query = templater.createQuery(featureSet, startDate, endDate, limit);
      String tableName = createBqTableName(datasetPrefix, featureSet, startDate, endDate,
          namePrefix);
      String tableDescription = createBqTableDescription(featureSet, startDate, endDate, query);

      Map<String, String> options = templater.getStorageSpec().getOptionsMap();
      String bq_dataset = options.get("dataset");

      TableId destinationTableId =
          TableId.of(projectId, bq_dataset, tableName);

      if (bigQuery.getTable(destinationTableId) == null) {
        QueryJobConfiguration queryConfig =
            QueryJobConfiguration.newBuilder(query)
                .setAllowLargeResults(true)
                .setDestinationTable(destinationTableId)
                .build();
        JobOption jobOption = JobOption.fields();
        TableResult res = bigQuery.query(queryConfig, jobOption);
        if (res != null) {
          Table destinationTable = bigQuery.getTable(destinationTableId);
          TableInfo tableInfo = destinationTable.toBuilder()
              .setDescription(tableDescription)
              .build();
          bigQuery.update(tableInfo);
        }
      }

      return DatasetInfo.newBuilder()
          .setName(tableName)
          .setTableUrl(toTableUrl(destinationTableId))
          .build();
    } catch (JobException e) {
      log.error("Failed creating training dataset", e);
      throw new TrainingDatasetCreationException("Failed creating training dataset", e);
    } catch (InterruptedException e) {
      log.error("Training dataset creation was interrupted", e);
      throw new TrainingDatasetCreationException("Training dataset creation was interrupted", e);
    }
  }

  private String createBqTableName(String datasetPrefix, FeatureSet featureSet, Timestamp startDate,
      Timestamp endDate, String namePrefix) {

    List<String> features = new ArrayList(featureSet.getFeatureIdsList());
    Collections.sort(features);

    String datasetId = String.format("%s_%s_%s", features, startDate, endDate);
    StringBuilder hashText;

    // create hash from datasetId
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-1");
      byte[] messageDigest = md.digest(datasetId.getBytes());
      BigInteger no = new BigInteger(1, messageDigest);
      hashText = new StringBuilder(no.toString(16));
      while (hashText.length() < 32) {
        hashText.insert(0, "0");
      }
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }

    if (!Strings.isNullOrEmpty(namePrefix)) {
      //  only alphanumeric and underscore are allowed
      namePrefix = namePrefix.replaceAll("[^a-zA-Z0-9_]", "_");
      return String.format(
          "%s_%s_%s_%s", datasetPrefix, featureSet.getEntityName(), namePrefix,
          hashText.toString());
    }

    return String.format(
        "%s_%s_%s", datasetPrefix, featureSet.getEntityName(), hashText.toString());
  }

  private String createBqTableDescription(FeatureSet featureSet, Timestamp startDate, Timestamp
      endDate, String query) {
    String currentTime = Instant.now().toString();
    return new StringBuilder()
        .append("Feast Dataset for ")
        .append(featureSet.getEntityName())
        .append(" features.\nContains data from ")
        .append(formatTimestamp(startDate))
        .append(" to ")
        .append(formatTimestamp(endDate))
        .append(".\nLast edited at ")
        .append(currentTime)
        .append(".\n\n-----\n\n")
        .append(query)
        .toString();
  }

  private String formatTimestamp(Timestamp timestamp) {
    Instant instant = Instant.ofEpochSecond(timestamp.getSeconds());
    return formatter.format(instant);
  }

  private String toTableUrl(TableId tableId) {
    return String.format(
        "%s.%s.%s", tableId.getProject(), tableId.getDataset(), tableId.getTable());
  }

}
