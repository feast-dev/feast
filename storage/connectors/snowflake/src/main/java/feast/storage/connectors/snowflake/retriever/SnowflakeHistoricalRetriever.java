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

import feast.proto.serving.ServingAPIProto;
import feast.storage.api.retriever.FeatureSetRequest;
import feast.storage.api.retriever.HistoricalRetrievalResult;
import feast.storage.api.retriever.HistoricalRetriever;
import feast.storage.connectors.snowflake.snowflake.TimestampLimits;
import java.util.*;

public class SnowflakeHistoricalRetriever implements HistoricalRetriever {

  private final String stagingLocation;
  private JdbcQueryTemplater queryTemplater;

  private SnowflakeHistoricalRetriever(
      Map<String, String> config, JdbcQueryTemplater queryTemplater) {

    this.stagingLocation = config.get("staging_location");
    this.queryTemplater = queryTemplater;
  }

  public static HistoricalRetriever create(
      Map<String, String> config, JdbcQueryTemplater jdbcQueryTemplater) {
    return new SnowflakeHistoricalRetriever(config, jdbcQueryTemplater);
  }

  // TODO: implement computeStatistics
  @Override
  public HistoricalRetrievalResult getHistoricalFeatures(
      String retrievalId,
      ServingAPIProto.DatasetSource datasetSource,
      List<FeatureSetRequest> featureSetRequests,
      boolean computeStatistics) {

    // 1. Ensure correct entity row file format
    ServingAPIProto.DataFormat dataFormat = datasetSource.getFileSource().getDataFormat();
    if (!dataFormat.equals(ServingAPIProto.DataFormat.DATA_FORMAT_CSV)) {
      throw new IllegalArgumentException(
          String.format(
              "Only CSV imports are allows for JDBC sources. Type provided was %s",
              dataFormat.name()));
    }

    // 2. Build feature set query infos
    List<FeatureSetQueryInfo> featureSetQueryInfos =
        this.queryTemplater.getFeatureSetInfos(featureSetRequests);

    // 3. Load entity rows into database
    List<String> fileList = datasetSource.getFileSource().getFileUrisList();
    String entityTableWithRowCountName =
        this.queryTemplater.loadEntities(featureSetQueryInfos, fileList, stagingLocation);

    // 4. Retrieve the temporal bounds of the entity dataset provided
    TimestampLimits timestampLimits =
        this.queryTemplater.getTimestampLimits(entityTableWithRowCountName);

    // 5. Generate the subqueries
    List<String> featureSetQueries =
        this.queryTemplater.generateFeatureSetQueries(
            entityTableWithRowCountName, timestampLimits, featureSetQueryInfos);

    // 6. Run the subqueries and collect outputs
    String resultTable =
        this.queryTemplater.runBatchQuery(
            entityTableWithRowCountName, featureSetQueryInfos, featureSetQueries);

    // 7. export the result feature as a csv file to staging location
    String fileUri =
        this.queryTemplater.exportResultTableToStagingLocation(resultTable, stagingLocation);
    List<String> fileUris = new ArrayList<>();
    // TODO: always return a single csv file?
    fileUris.add(fileUri);
    return HistoricalRetrievalResult.success(
        retrievalId, fileUris, ServingAPIProto.DataFormat.DATA_FORMAT_CSV);
  }

  @Override
  public String getStagingLocation() {
    return this.stagingLocation;
  }
}
