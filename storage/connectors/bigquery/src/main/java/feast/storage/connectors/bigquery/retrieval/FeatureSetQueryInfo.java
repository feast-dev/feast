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

import java.util.List;

public class FeatureSetQueryInfo {

  private final String project;
  private final String name;
  private final int version;
  private final long maxAge;
  private final List<String> entities;
  private final List<String> features;
  private final String table;

  public FeatureSetQueryInfo(
      String project,
      String name,
      int version,
      long maxAge,
      List<String> entities,
      List<String> features,
      String table) {
    this.project = project;
    this.name = name;
    this.version = version;
    this.maxAge = maxAge;
    this.entities = entities;
    this.features = features;
    this.table = table;
  }

  public FeatureSetQueryInfo(FeatureSetQueryInfo featureSetInfo, String table) {

    this.project = featureSetInfo.getProject();
    this.name = featureSetInfo.getName();
    this.version = featureSetInfo.getVersion();
    this.maxAge = featureSetInfo.getMaxAge();
    this.entities = featureSetInfo.getEntities();
    this.features = featureSetInfo.getFeatures();
    this.table = table;
  }

  public String getProject() {
    return project;
  }

  public String getName() {
    return name;
  }

  public int getVersion() {
    return version;
  }

  public long getMaxAge() {
    return maxAge;
  }

  public List<String> getEntities() {
    return entities;
  }

  public List<String> getFeatures() {
    return features;
  }

  public String getTable() {
    return table;
  }
}
