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
package feast.storage.connectors.bigquery.statistics;

import com.mitchellbosecke.pebble.PebbleEngine;
import com.mitchellbosecke.pebble.template.PebbleTemplate;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

public class StatsQueryTemplater {

  private static final PebbleEngine engine = new PebbleEngine.Builder().build();
  private static final String BASIC_STATS_TEMPLATE_NAME = "templates/basic_stats.sql";
  private static final String HIST_STATS_TEMPLATE_NAME = "templates/hist_stats.sql";

  /**
   * Generate the query for getting basic statistics about a given feature set
   *
   * @param featureSetInfo Information about the feature set necessary for the query templating
   * @param projectId google project ID
   * @param datasetId feast bigquery dataset ID
   * @return point in time correctness join BQ SQL query
   */
  public static String createGetFeatureSetStatsQuery(
      FeatureSetStatisticsQueryInfo featureSetInfo, String projectId, String datasetId)
      throws IOException {

    PebbleTemplate template = engine.getTemplate(BASIC_STATS_TEMPLATE_NAME);
    Map<String, Object> context = new HashMap<>();
    context.put("featureSet", featureSetInfo);
    context.put("projectId", projectId);
    context.put("datasetId", datasetId);

    Writer writer = new StringWriter();
    template.evaluate(writer, context);
    return writer.toString();
  }

  /**
   * Generate the query for getting histograms for features in a given feature set
   *
   * @param featureSetInfo Information about the feature set necessary for the query templating
   * @param projectId google project ID
   * @param datasetId feast bigquery dataset ID
   * @return point in time correctness join BQ SQL query
   */
  public static String createGetFeatureSetHistQuery(
      FeatureSetStatisticsQueryInfo featureSetInfo, String projectId, String datasetId)
      throws IOException {

    PebbleTemplate template = engine.getTemplate(HIST_STATS_TEMPLATE_NAME);
    Map<String, Object> context = new HashMap<>();
    context.put("featureSet", featureSetInfo);
    context.put("projectId", projectId);
    context.put("datasetId", datasetId);

    Writer writer = new StringWriter();
    template.evaluate(writer, context);
    return writer.toString();
  }
}
