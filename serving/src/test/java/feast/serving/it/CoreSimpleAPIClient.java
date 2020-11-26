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
package feast.serving.it;

import feast.proto.core.CoreServiceGrpc;
import feast.proto.core.CoreServiceProto;
import feast.proto.core.EntityProto;
import feast.proto.core.FeatureTableProto;

public class CoreSimpleAPIClient {
  private CoreServiceGrpc.CoreServiceBlockingStub stub;

  public CoreSimpleAPIClient(CoreServiceGrpc.CoreServiceBlockingStub stub) {
    this.stub = stub;
  }

  public void simpleApplyEntity(String projectName, EntityProto.EntitySpecV2 entitySpec) {
    stub.applyEntity(
        CoreServiceProto.ApplyEntityRequest.newBuilder()
            .setProject(projectName)
            .setSpec(entitySpec)
            .build());
  }

  public EntityProto.Entity getEntity(String projectName, String name) {
    return stub.getEntity(
            CoreServiceProto.GetEntityRequest.newBuilder()
                .setProject(projectName)
                .setName(name)
                .build())
        .getEntity();
  }

  public void simpleApplyFeatureTable(
      String projectName, FeatureTableProto.FeatureTableSpec featureTable) {
    stub.applyFeatureTable(
        CoreServiceProto.ApplyFeatureTableRequest.newBuilder()
            .setProject(projectName)
            .setTableSpec(featureTable)
            .build());
  }

  public FeatureTableProto.FeatureTable simpleGetFeatureTable(String projectName, String name) {
    return stub.getFeatureTable(
            CoreServiceProto.GetFeatureTableRequest.newBuilder()
                .setName(name)
                .setProject(projectName)
                .build())
        .getTable();
  }
}
