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

import com.google.protobuf.Timestamp;
import feast.proto.core.FeatureSetProto.EntitySpec;
import feast.proto.core.FeatureSetProto.FeatureSpec;
import java.util.ArrayList;
import java.util.List;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Value class for Feature Sets containing information necessary to template stats-retrieving
 * queries.
 */
public class FeatureSetStatisticsQueryInfo {
  // Feast project name
  private final String project;

  // Feature set name
  private final String name;

  // Dataset ID to retrieve statistics over
  private String datasetId = "";

  // Date to retrieve statistics over
  private String date = "";

  // List of entity names in this feature set
  private final List<String> entityNames;

  // List of fields to get stats for
  private final List<FeatureStatisticsQueryInfo> features;

  public FeatureSetStatisticsQueryInfo(String project, String name, String datasetId) {
    this.project = project;
    this.name = name;
    this.entityNames = new ArrayList<>();
    this.features = new ArrayList<>();
    this.datasetId = datasetId;
  }

  public FeatureSetStatisticsQueryInfo(String project, String name, Timestamp date) {
    this.project = project;
    this.name = name;
    this.entityNames = new ArrayList<>();
    this.features = new ArrayList<>();
    DateTime dateTime = new DateTime(date.getSeconds() * 1000, DateTimeZone.UTC);
    DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd");
    this.date = fmt.print(dateTime);
  }

  public void addFeature(FeatureSpec featureSpec) {
    this.features.add(FeatureStatisticsQueryInfo.fromProto(featureSpec));
  }

  public void addEntity(EntitySpec entitySpec) {
    this.entityNames.add(entitySpec.getName());
    this.features.add(FeatureStatisticsQueryInfo.fromProto(entitySpec));
  }

  public String getProject() {
    return project;
  }

  public String getName() {
    return name;
  }

  public String getDatasetId() {
    return datasetId;
  }

  public String getDate() {
    return date;
  }

  public List<String> getEntityNames() {
    return entityNames;
  }

  public List<FeatureStatisticsQueryInfo> getFeatures() {
    return features;
  }
}
