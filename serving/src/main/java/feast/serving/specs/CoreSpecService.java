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

import feast.core.CoreServiceGrpc;
import feast.core.CoreServiceProto.GetFeatureSetRequest;
import feast.core.CoreServiceProto.GetFeatureSetResponse;
import feast.core.CoreServiceProto.ListFeatureSetsRequest;
import feast.core.CoreServiceProto.ListFeatureSetsResponse;
import feast.core.CoreServiceProto.UpdateStoreRequest;
import feast.core.CoreServiceProto.UpdateStoreResponse;
import feast.core.StoreProto.Store;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;

/** Client for interfacing with specs in Feast Core. */
public class CoreSpecService {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(CoreSpecService.class);
  private final CoreServiceGrpc.CoreServiceBlockingStub blockingStub;

  public CoreSpecService(String feastCoreHost, int feastCorePort) {
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress(feastCoreHost, feastCorePort).usePlaintext().build();
    blockingStub = CoreServiceGrpc.newBlockingStub(channel);
  }

  public GetFeatureSetResponse getFeatureSet(GetFeatureSetRequest getFeatureSetRequest) {
    return blockingStub.getFeatureSet(getFeatureSetRequest);
  }

  public ListFeatureSetsResponse listFeatureSets(ListFeatureSetsRequest ListFeatureSetsRequest) {
    return blockingStub.listFeatureSets(ListFeatureSetsRequest);
  }

  public UpdateStoreResponse updateStore(UpdateStoreRequest updateStoreRequest) {
    return blockingStub.updateStore(updateStoreRequest);
  }

  /**
   * Register the given store entry in Feast Core. If store already exists in Feast Core, updates
   * the store entry in feast core.
   *
   * @param store entry to register/update in Feast Core.
   * @return The register/updated store entry
   */
  public Store registerStore(Store store) {
    UpdateStoreRequest request = UpdateStoreRequest.newBuilder().setStore(store).build();
    try {
      UpdateStoreResponse updateStoreResponse = this.updateStore(request);
      if (!updateStoreResponse.getStore().equals(store)) {
        throw new RuntimeException("Core store config not matching current store config");
      }
      return updateStoreResponse.getStore();
    } catch (Exception e) {
      throw new RuntimeException("Unable to update store configuration", e);
    }
  }
}
