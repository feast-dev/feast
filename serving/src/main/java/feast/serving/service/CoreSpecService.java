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
package feast.serving.service;

import feast.core.CoreServiceGrpc;
import feast.core.CoreServiceProto.ListFeatureSetsRequest;
import feast.core.CoreServiceProto.ListFeatureSetsResponse;
import feast.core.CoreServiceProto.UpdateStoreRequest;
import feast.core.CoreServiceProto.UpdateStoreResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;

/** Client for spec retrieval from core. */
@Slf4j
public class CoreSpecService {
  private final CoreServiceGrpc.CoreServiceBlockingStub blockingStub;

  public CoreSpecService(String feastCoreHost, int feastCorePort) {
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress(feastCoreHost, feastCorePort).usePlaintext().build();
    blockingStub = CoreServiceGrpc.newBlockingStub(channel);
  }

  public ListFeatureSetsResponse listFeatureSets(ListFeatureSetsRequest ListFeatureSetsRequest) {
    return blockingStub.listFeatureSets(ListFeatureSetsRequest);
  }

  public UpdateStoreResponse updateStore(UpdateStoreRequest updateStoreRequest) {
    return blockingStub.updateStore(updateStoreRequest);
  }
}
