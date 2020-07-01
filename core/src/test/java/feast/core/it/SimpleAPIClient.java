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
package feast.core.it;

import feast.proto.core.*;
import feast.proto.types.ValueProto;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;

public class SimpleAPIClient {
  private CoreServiceGrpc.CoreServiceBlockingStub stub;

  public SimpleAPIClient(CoreServiceGrpc.CoreServiceBlockingStub stub) {
    this.stub = stub;
  }

  public void simpleApplyFeatureSet(
      SourceProto.Source source,
      String projectName,
      String name,
      List<Pair<String, ValueProto.ValueType.Enum>> entities,
      List<Pair<String, ValueProto.ValueType.Enum>> features) {
    stub.applyFeatureSet(
        CoreServiceProto.ApplyFeatureSetRequest.newBuilder()
            .setFeatureSet(
                FeatureSetProto.FeatureSet.newBuilder()
                    .setSpec(
                        FeatureSetProto.FeatureSetSpec.newBuilder()
                            .setSource(source)
                            .setName(name)
                            .setProject(projectName)
                            .addAllEntities(
                                entities.stream()
                                    .map(
                                        pair ->
                                            FeatureSetProto.EntitySpec.newBuilder()
                                                .setName(pair.getLeft())
                                                .setValueType(pair.getRight())
                                                .build())
                                    .collect(Collectors.toList()))
                            .addAllFeatures(
                                features.stream()
                                    .map(
                                        pair ->
                                            FeatureSetProto.FeatureSpec.newBuilder()
                                                .setName(pair.getLeft())
                                                .setValueType(pair.getRight())
                                                .build())
                                    .collect(Collectors.toList()))
                            .build())
                    .build())
            .build());
  }

  public List<FeatureSetProto.FeatureSet> simpleListFeatureSets(String name) {
    return stub.listFeatureSets(
            CoreServiceProto.ListFeatureSetsRequest.newBuilder()
                .setFilter(
                    CoreServiceProto.ListFeatureSetsRequest.Filter.newBuilder()
                        .setFeatureSetName(name)
                        .build())
                .build())
        .getFeatureSetsList();
  }

  public FeatureSetProto.FeatureSet simpleGetFeatureSet(String projectName, String name) {
    return stub.getFeatureSet(
            CoreServiceProto.GetFeatureSetRequest.newBuilder()
                .setName(name)
                .setProject(projectName)
                .build())
        .getFeatureSet();
  }

  public void updateStore(StoreProto.Store store) {
    stub.updateStore(CoreServiceProto.UpdateStoreRequest.newBuilder().setStore(store).build());
  }

  public void createProject(String name) {
    stub.createProject(CoreServiceProto.CreateProjectRequest.newBuilder().setName(name).build());
  }

  public void restartIngestionJob(String jobId) {
    stub.restartIngestionJob(
        CoreServiceProto.RestartIngestionJobRequest.newBuilder().setId(jobId).build());
  }

  public List<IngestionJobProto.IngestionJob> listIngestionJobs() {
    return stub.listIngestionJobs(
            CoreServiceProto.ListIngestionJobsRequest.newBuilder()
                .setFilter(CoreServiceProto.ListIngestionJobsRequest.Filter.newBuilder().build())
                .build())
        .getJobsList();
  }
}
