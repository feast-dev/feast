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
package feast.storage.connectors.snowflake.retriever;

import feast.proto.core.FeatureSetProto;
import feast.proto.serving.ServingAPIProto;
import feast.storage.api.retriever.FeatureSetRequest;
import feast.storage.connectors.snowflake.snowflake.TimestampLimits;
import java.util.List;

public interface JdbcQueryTemplater {

  /**
   * Generate the information necessary for the sql templating for point in time correctness join to
   * the entity dataset for each feature set requested.
   *
   * @param featureSetRequests List of {@link FeatureSetRequest} containing a {@link
   *     FeatureSetProto.FeatureSetSpec} and its corresponding {@link
   *     ServingAPIProto.FeatureReference}s provided by the user.
   * @return List of {@link FeatureSetQueryInfo}FeatureSetInfos
   */
  List<FeatureSetQueryInfo> getFeatureSetInfos(List<FeatureSetRequest> featureSetRequests);

  /**
   * Load entity rows into database
   *
   * @param featureSetQueryInfos
   * @param entitySourceUris list of files in the {@link ServingAPIProto.DatasetSource} which
   *     contain entity rows (csv files with delimiter="," and two columns in order:
   *     entity_id_primary, event_timestamp)
   * @param stagingUri staging uri, eg: a S3 bucket or GCP location
   * @return entityTableWithRowCountName: the name of the entity Table in the database With RowCount
   *     column (for batch job retrieve)
   */
  String loadEntities(
      List<FeatureSetQueryInfo> featureSetQueryInfos,
      List<String> entitySourceUris,
      String stagingUri);

  /**
   * Retrieve the temporal bounds of the entity dataset provided
   *
   * @param entityTableWithRowCountName entity table name
   * @return a timestamp bounds map with max, and min as keys
   */
  TimestampLimits getTimestampLimits(String entityTableWithRowCountName);

  /**
   * Generate the queries for point in time correctness join of data for the given feature sets to
   * the entity dataset.
   *
   * @param entityTableWithRowCountName entity table name
   * @param timestampLimits a timestamp bounds map with max, and min as keys
   * @param featureSetQueryInfos a list of Information about the feature set necessary for the query
   *     templating
   * @return a list of point in time correctness join BQ SQL query (featureSetQueries)
   */
  List<String> generateFeatureSetQueries(
      String entityTableWithRowCountName,
      TimestampLimits timestampLimits,
      List<FeatureSetQueryInfo> featureSetQueryInfos);

  /**
   * Run each featureset query request as a batch job, and create the output featureset table in the
   * database
   *
   * @param entityTableName a list of Information about the feature set necessary for the query
   *     templating
   * @param featureSetQueryInfos a list of Information about the feature set necessary for the query
   *     templating
   * @param featureSetQueries a list of point in time correctness join BQ SQL query
   *     (featureSetQueries)
   * @return the name of the resultTable in the database
   */
  String runBatchQuery(
      String entityTableName,
      List<FeatureSetQueryInfo> featureSetQueryInfos,
      List<String> featureSetQueries);

  /**
   * Export the given result table in the database to the staging location as a CSV file
   *
   * @param resultTable name of the result table in the database
   * @param stagingUri staging uri, eg: a S3 bucket or GCP location
   * @return the fileUri of the exported csv file in the staging location
   */
  String exportResultTableToStagingLocation(String resultTable, String stagingUri);
}
