/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
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
package feast.storage.connectors.bigquery.retrieval;

import com.google.cloud.bigquery.TableId;
import com.google.protobuf.Duration;
import com.mitchellbosecke.pebble.PebbleEngine;
import com.mitchellbosecke.pebble.template.PebbleTemplate;
import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.serving.ServingAPIProto.FeatureReference;
import feast.storage.api.retrieval.FeatureSetRequest;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class QueryTemplater {

  private static final PebbleEngine engine = new PebbleEngine.Builder().build();
  private static final String FEATURESET_TEMPLATE_NAME = "templates/single_featureset_pit_join.sql";
  private static final String JOIN_TEMPLATE_NAME = "templates/join_featuresets.sql";

  /**
   * Get the query for retrieving the earliest and latest timestamps in the entity dataset.
   *
   * @param leftTableName full entity dataset name
   * @return timestamp limit BQ SQL query
   */
  public static String createTimestampLimitQuery(String leftTableName) {
    return String.format(
        "SELECT DATETIME(MAX(event_timestamp)) as max, DATETIME(MIN(event_timestamp)) as min FROM `%s`",
        leftTableName);
  }

  /**
   * Creates a query that generates a UUID for the entity table, for left joins later on.
   *
   * @param leftTableName full entity dataset name
   * @return uuid generation query
   */
  public static String createEntityTableUUIDQuery(String leftTableName) {
    return String.format(
        "SELECT GENERATE_UUID() as uuid, `%s`.* from `%s`", leftTableName, leftTableName);
  }

  /**
   * Generate the information necessary for the sql templating for point in time correctness join to
   * the entity dataset for each feature set requested.
   *
   * @param featureSetRequests List of {@link FeatureSetRequest} containing a {@link FeatureSetSpec}
   *     and its corresponding {@link FeatureReference}s provided by the user.
   * @return List of FeatureSetInfos
   */
  public static List<FeatureSetQueryInfo> getFeatureSetInfos(
      List<FeatureSetRequest> featureSetRequests) throws IllegalArgumentException {

    List<FeatureSetQueryInfo> featureSetInfos = new ArrayList<>();
    for (FeatureSetRequest featureSetRequest : featureSetRequests) {
      FeatureSetSpec spec = featureSetRequest.getSpec();
      Duration maxAge = spec.getMaxAge();
      List<String> fsEntities =
          spec.getEntitiesList().stream().map(EntitySpec::getName).collect(Collectors.toList());
      List<String> features =
          featureSetRequest.getFeatureReferences().stream()
              .map(FeatureReference::getName)
              .collect(Collectors.toList());
      featureSetInfos.add(
          new FeatureSetQueryInfo(
              spec.getProject(),
              spec.getName(),
              spec.getVersion(),
              maxAge.getSeconds(),
              fsEntities,
              features,
              ""));
    }
    return featureSetInfos;
  }

  /**
   * Generate the query for point in time correctness join of data for a single feature set to the
   * entity dataset.
   *
   * @param featureSetInfo Information about the feature set necessary for the query templating
   * @param projectId google project ID
   * @param datasetId feast bigquery dataset ID
   * @param leftTableName entity dataset name
   * @param minTimestamp earliest allowed timestamp for the historical data in feast
   * @param maxTimestamp latest allowed timestamp for the historical data in feast
   * @return point in time correctness join BQ SQL query
   */
  public static String createFeatureSetPointInTimeQuery(
      FeatureSetQueryInfo featureSetInfo,
      String projectId,
      String datasetId,
      String leftTableName,
      String minTimestamp,
      String maxTimestamp)
      throws IOException {

    PebbleTemplate template = engine.getTemplate(FEATURESET_TEMPLATE_NAME);
    Map<String, Object> context = new HashMap<>();
    context.put("featureSet", featureSetInfo);
    context.put("projectId", projectId);
    context.put("datasetId", datasetId);
    context.put("minTimestamp", minTimestamp);
    context.put("maxTimestamp", maxTimestamp);
    context.put("leftTableName", leftTableName);

    Writer writer = new StringWriter();
    template.evaluate(writer, context);
    return writer.toString();
  }

  /**
   * @param featureSetInfos List of FeatureSetInfos containing information about the feature set
   *     necessary for the query templating
   * @param entityTableColumnNames list of column names in entity table
   * @param leftTableName entity dataset name
   * @return query to join temporary feature set tables to the entity table
   */
  public static String createJoinQuery(
      List<FeatureSetQueryInfo> featureSetInfos,
      List<String> entityTableColumnNames,
      String leftTableName)
      throws IOException {
    PebbleTemplate template = engine.getTemplate(JOIN_TEMPLATE_NAME);
    Map<String, Object> context = new HashMap<>();
    context.put("entities", entityTableColumnNames);
    context.put("featureSets", featureSetInfos);
    context.put("leftTableName", leftTableName);

    Writer writer = new StringWriter();
    template.evaluate(writer, context);
    return writer.toString();
  }

  public static String generateFullTableName(TableId tableId) {
    return String.format(
        "%s.%s.%s", tableId.getProject(), tableId.getDataset(), tableId.getTable());
  }
}
