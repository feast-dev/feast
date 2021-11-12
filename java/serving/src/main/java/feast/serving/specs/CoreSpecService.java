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
package feast.serving.specs;

import feast.proto.core.CoreServiceGrpc;
import feast.proto.core.CoreServiceProto;
import feast.proto.core.CoreServiceProto.ListFeatureTablesRequest;
import feast.proto.core.CoreServiceProto.ListFeatureTablesResponse;
import feast.proto.core.CoreServiceProto.ListProjectsRequest;
import feast.proto.core.CoreServiceProto.ListProjectsResponse;
import feast.proto.core.CoreServiceProto.UpdateStoreRequest;
import feast.proto.core.CoreServiceProto.UpdateStoreResponse;
import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.springframework.beans.factory.ObjectProvider;

/** Client for interfacing with specs in Feast Core. */
public class CoreSpecService {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(CoreSpecService.class);
  private final CoreServiceGrpc.CoreServiceBlockingStub blockingStub;

  public CoreSpecService(
      String feastCoreHost, int feastCorePort, ObjectProvider<CallCredentials> callCredentials) {
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress(feastCoreHost, feastCorePort).usePlaintext().build();
    CallCredentials creds = callCredentials.getIfAvailable();
    if (creds != null) {
      blockingStub = CoreServiceGrpc.newBlockingStub(channel).withCallCredentials(creds);
    } else {
      blockingStub = CoreServiceGrpc.newBlockingStub(channel);
    }
  }

  public UpdateStoreResponse updateStore(UpdateStoreRequest updateStoreRequest) {
    return blockingStub.updateStore(updateStoreRequest);
  }

  public ListProjectsResponse listProjects(ListProjectsRequest listProjectsRequest) {
    return blockingStub.listProjects(listProjectsRequest);
  }

  public ListFeatureTablesResponse listFeatureTables(
      ListFeatureTablesRequest listFeatureTablesRequest) {
    return blockingStub.listFeatureTables(listFeatureTablesRequest);
  }

  public CoreServiceProto.GetFeatureTableResponse getFeatureTable(
      CoreServiceProto.GetFeatureTableRequest getFeatureTableRequest) {
    return blockingStub.getFeatureTable(getFeatureTableRequest);
  }
}
