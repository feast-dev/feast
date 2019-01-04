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
import com.google.cloud.bigquery.JobException;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.common.base.Strings;
import com.google.protobuf.Timestamp;
import feast.core.TrainingServiceProto.DatasetInfo;
import feast.core.TrainingServiceProto.FeatureSet;
import feast.core.exception.TrainingDatasetCreationException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BigQueryTrainingDatasetCreator {

  private final String projectId;
  private final String datasetPrefix;
  private final BigQueryTrainingDatasetTemplater templater;
  private final BigQuery bigQuery;
  private final DateTimeFormatter formatter;
  private final Clock clock;

  public BigQueryTrainingDatasetCreator(
      BigQueryTrainingDatasetTemplater templater,
      BigQuery bigQuery,
      Clock clock,
      String projectId,
      String datasetPrefix) {
    this.templater = templater;
    this.projectId = projectId;
    this.datasetPrefix = datasetPrefix;
    this.bigQuery = bigQuery;
    this.clock = clock;
    this.formatter = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneId.of("UTC"));
  }

  /**
   * Create training dataset for a feature set
   *
   * @param featureSet feature set for which the training dataset should be created
   * @param startDate starting date of the training dataset (inclusive)
   * @param endDate end date of the training dataset (inclusive)
   * @param limit maximum number of row should be created.
   * @param namePrefix prefix for dataset name
   * @return dataset info associated with the created training dataset
   */
  public DatasetInfo createTrainingDataset(
      FeatureSet featureSet,
      Timestamp startDate,
      Timestamp endDate,
      long limit,
      String namePrefix) {
    try {
      String query = templater.createQuery(featureSet, startDate, endDate, limit);

      String tableName = createBqTableName(startDate, endDate, namePrefix);
      TableId destinationTable =
          TableId.of(projectId, createBqDatasetName(featureSet.getEntityName()), tableName);
      QueryJobConfiguration queryConfig =
          QueryJobConfiguration.newBuilder(query)
              .setAllowLargeResults(true)
              .setDestinationTable(destinationTable)
              .build();
      JobOption jobOption = JobOption.fields();
      bigQuery.query(queryConfig, jobOption);
      return DatasetInfo.newBuilder()
          .setName(createTrainingDatasetName(namePrefix, featureSet.getEntityName(), tableName))
          .setTableUrl(toTableUrl(destinationTable))
          .build();
    } catch (JobException e) {
      log.error("Failed creating training dataset", e);
      throw new TrainingDatasetCreationException("Failed creating training dataset", e);
    } catch (InterruptedException e) {
      log.error("Training dataset creation was interrupted", e);
      throw new TrainingDatasetCreationException("Training dataset creation was interrupted", e);
    }
  }

  private String createBqTableName(Timestamp startDate, Timestamp endDate, String namePrefix) {
    String currentTime = String.valueOf(clock.millis());
    if (!Strings.isNullOrEmpty(namePrefix)) {
      //  only alphanumeric and underscore are allowed
      namePrefix = namePrefix.replaceAll("[^a-zA-Z0-9_]", "_");
      return String.format(
          "%s_%s_%s_%s",
          namePrefix, currentTime, formatTimestamp(startDate), formatTimestamp(endDate));
    }

    return String.format(
        "%s_%s_%s", currentTime, formatTimestamp(startDate), formatTimestamp(endDate));
  }

  private String createBqDatasetName(String entity) {
    return String.format("%s_%s", datasetPrefix, entity);
  }

  private String formatTimestamp(Timestamp timestamp) {
    Instant instant = Instant.ofEpochSecond(timestamp.getSeconds());
    return formatter.format(instant);
  }

  private String toTableUrl(TableId tableId) {
    return String.format(
        "%s.%s.%s", tableId.getProject(), tableId.getDataset(), tableId.getTable());
  }

  private String createTrainingDatasetName(String namePrefix, String entityName, String tableName) {
    if (!Strings.isNullOrEmpty(namePrefix)) {
      return tableName;
    }
    return String.format("%s_%s", entityName, tableName);
  }
}
