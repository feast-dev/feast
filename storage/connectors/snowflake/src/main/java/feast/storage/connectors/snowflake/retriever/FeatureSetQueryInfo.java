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
package feast.storage.connectors.snowflake.retriever;

import feast.proto.core.FeatureSetProto;
import feast.proto.serving.ServingAPIProto.FeatureReference;
import feast.storage.connectors.snowflake.common.DatabaseTemplater;
import java.util.List;

public class FeatureSetQueryInfo {

  private final String project;
  private final String name;
  private final long maxAge;
  private final List<String> entities;
  private final List<FeatureReference> features;
  private final String featureSetTable;
  private String joinedTable;

  public FeatureSetQueryInfo(
      String project,
      String name,
      long maxAge,
      List<String> entities,
      List<FeatureReference> features) {
    this.project = project;
    this.name = name;
    this.maxAge = maxAge;
    this.entities = entities;
    this.features = features;
    this.featureSetTable =
        DatabaseTemplater.getTableName(
            FeatureSetProto.FeatureSetSpec.newBuilder().setName(name).setProject(project).build());
  }

  public String getProject() {
    return project;
  }

  public String getName() {
    return name;
  }

  public List<String> getEntities() {
    return entities;
  }

  public List<FeatureReference> getFeatures() {
    return features;
  }

  public String getFeatureSetTable() {
    return featureSetTable;
  }

  public String getJoinedTable() {
    return joinedTable;
  }

  public void setJoinedTable(String joinedTable) {
    this.joinedTable = joinedTable;
  }

  public long getMaxAge() {
    return maxAge;
  }
}
