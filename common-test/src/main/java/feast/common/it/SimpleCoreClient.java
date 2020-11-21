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
package feast.common.it;

import feast.proto.core.*;
import feast.proto.core.CoreServiceProto.ApplyFeatureTableRequest;
import feast.proto.core.FeatureTableProto.FeatureTableSpec;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SimpleCoreClient {
  private CoreServiceGrpc.CoreServiceBlockingStub stub;

  public SimpleCoreClient(CoreServiceGrpc.CoreServiceBlockingStub stub) {
    this.stub = stub;
  }

  public CoreServiceProto.ApplyEntityResponse simpleApplyEntity(
      String projectName, EntityProto.EntitySpecV2 spec) {
    return stub.applyEntity(
        CoreServiceProto.ApplyEntityRequest.newBuilder()
            .setProject(projectName)
            .setSpec(spec)
            .build());
  }

  public List<EntityProto.Entity> simpleListEntities(String projectName) {
    return stub.listEntities(
            CoreServiceProto.ListEntitiesRequest.newBuilder()
                .setFilter(
                    CoreServiceProto.ListEntitiesRequest.Filter.newBuilder()
                        .setProject(projectName)
                        .build())
                .build())
        .getEntitiesList();
  }

  public List<EntityProto.Entity> simpleListEntities(
      String projectName, Map<String, String> labels) {
    return stub.listEntities(
            CoreServiceProto.ListEntitiesRequest.newBuilder()
                .setFilter(
                    CoreServiceProto.ListEntitiesRequest.Filter.newBuilder()
                        .setProject(projectName)
                        .putAllLabels(labels)
                        .build())
                .build())
        .getEntitiesList();
  }

  public List<EntityProto.Entity> simpleListEntities(
      CoreServiceProto.ListEntitiesRequest.Filter filter) {
    return stub.listEntities(
            CoreServiceProto.ListEntitiesRequest.newBuilder().setFilter(filter).build())
        .getEntitiesList();
  }

  public List<FeatureTableProto.FeatureTable> simpleListFeatureTables(
      CoreServiceProto.ListFeatureTablesRequest.Filter filter) {
    return stub.listFeatureTables(
            CoreServiceProto.ListFeatureTablesRequest.newBuilder().setFilter(filter).build())
        .getTablesList();
  }

  public EntityProto.Entity simpleGetEntity(String projectName, String name) {
    return stub.getEntity(
            CoreServiceProto.GetEntityRequest.newBuilder()
                .setName(name)
                .setProject(projectName)
                .build())
        .getEntity();
  }

  public FeatureTableProto.FeatureTable simpleGetFeatureTable(String projectName, String name) {
    return stub.getFeatureTable(
            CoreServiceProto.GetFeatureTableRequest.newBuilder()
                .setName(name)
                .setProject(projectName)
                .build())
        .getTable();
  }

  public Map<String, FeatureProto.FeatureSpecV2> simpleListFeatures(
      String projectName, Map<String, String> labels, List<String> entities) {
    return stub.listFeatures(
            CoreServiceProto.ListFeaturesRequest.newBuilder()
                .setFilter(
                    CoreServiceProto.ListFeaturesRequest.Filter.newBuilder()
                        .setProject(projectName)
                        .addAllEntities(entities)
                        .putAllLabels(labels)
                        .build())
                .build())
        .getFeaturesMap();
  }

  public Map<String, FeatureProto.FeatureSpecV2> simpleListFeatures(
      String projectName, String... entities) {
    return simpleListFeatures(projectName, Collections.emptyMap(), Arrays.asList(entities));
  }

  public CoreServiceProto.UpdateStoreResponse updateStore(StoreProto.Store store) {
    return stub.updateStore(
        CoreServiceProto.UpdateStoreRequest.newBuilder().setStore(store).build());
  }

  public void createProject(String name) {
    stub.createProject(CoreServiceProto.CreateProjectRequest.newBuilder().setName(name).build());
  }

  public void archiveProject(String name) {
    stub.archiveProject(CoreServiceProto.ArchiveProjectRequest.newBuilder().setName(name).build());
  }

  public String getFeastCoreVersion() {
    return stub.getFeastCoreVersion(
            CoreServiceProto.GetFeastCoreVersionRequest.getDefaultInstance())
        .getVersion();
  }

  public FeatureTableProto.FeatureTable applyFeatureTable(
      String projectName, FeatureTableSpec spec) {
    return stub.applyFeatureTable(
            ApplyFeatureTableRequest.newBuilder()
                .setProject(projectName)
                .setTableSpec(spec)
                .build())
        .getTable();
  }

  public void deleteFeatureTable(String projectName, String featureTableName) {
    stub.deleteFeatureTable(
        CoreServiceProto.DeleteFeatureTableRequest.newBuilder()
            .setProject(projectName)
            .setName(featureTableName)
            .build());
  }
}
