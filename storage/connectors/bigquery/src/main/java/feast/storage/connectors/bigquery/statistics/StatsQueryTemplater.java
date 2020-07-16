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
import java.util.List;
import java.util.Map;

public class StatsQueryTemplater {

  private static final PebbleEngine engine = new PebbleEngine.Builder().autoEscaping(false).build();
  private static final String BASIC_STATS_TEMPLATE_NAME = "templates/basic_stats.sql";
  private static final String HIST_STATS_TEMPLATE_NAME = "templates/hist_stats.sql";
  private static final String DATA_SUBSET_TEMPLATE_NAME = "templates/data_subset.sql";

  /**
   * Generate the query for getting basic statistics for a given set of features
   *
   * @param features Information about the features necessary for the query templating
   * @param statsDataset query selecting subset of data to compute statistics over
   * @return point in time correctness join BQ SQL query
   * @throws IOException
   */
  public static String createGetFeaturesStatsQuery(
      List<FeatureStatisticsQueryInfo> features, StatsDataset statsDataset) throws IOException {

    PebbleTemplate template = engine.getTemplate(BASIC_STATS_TEMPLATE_NAME);
    Map<String, Object> context = new HashMap<>();
    context.put("features", features);
    context.put("dataset", generateDataSubsetQuery(statsDataset));

    Writer writer = new StringWriter();
    template.evaluate(writer, context);
    return writer.toString();
  }

  /**
   * Generate the query for getting histograms for given set of features
   *
   * @param features Information about the features necessary for the query templating
   * @param statsDataset query selecting subset of data to compute statistics over
   * @return point in time correctness join BQ SQL query
   * @throws IOException
   */
  public static String createGetFeaturesHistQuery(
      List<FeatureStatisticsQueryInfo> features, StatsDataset statsDataset) throws IOException {

    PebbleTemplate template = engine.getTemplate(HIST_STATS_TEMPLATE_NAME);
    Map<String, Object> context = new HashMap<>();
    context.put("features", features);
    context.put("dataset", generateDataSubsetQuery(statsDataset));

    Writer writer = new StringWriter();
    template.evaluate(writer, context);
    return writer.toString();
  }

  /**
   * generate the query to subset the data to compute statistics over
   *
   * @param statsDataset {@link StatsDataset} describing the subset of data
   * @return BigQuery query selecting the data
   * @throws IOException
   */
  private static String generateDataSubsetQuery(StatsDataset statsDataset) throws IOException {
    PebbleTemplate template = engine.getTemplate(DATA_SUBSET_TEMPLATE_NAME);

    Writer writer = new StringWriter();
    template.evaluate(writer, statsDataset.getMap());
    return writer.toString();
  }
}
