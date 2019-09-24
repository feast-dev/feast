/*
 * Copyright 2018 The Feast Authors
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
 *
 */

package feast.serving.service.spec;

import feast.core.CoreServiceGrpc;
import feast.core.CoreServiceProto.GetFeatureSetsRequest;
import feast.core.CoreServiceProto.GetFeatureSetsResponse;
import feast.core.CoreServiceProto.GetStoresRequest;
import feast.core.CoreServiceProto.GetStoresRequest.Filter;
import feast.core.CoreServiceProto.GetStoresResponse;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.Subscription;
import feast.serving.exception.SpecRetrievalException;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/**
 * Class responsible for retrieving Feature, Entity, and Storage Spec from Feast Core service.
 */
@Slf4j
public class CoreSpecService implements SpecService {

  private final ManagedChannel channel;
  private final CoreServiceGrpc.CoreServiceBlockingStub blockingStub;

  public CoreSpecService(String host, int port) {
    this(ManagedChannelBuilder.forAddress(host, port));
  }

  public CoreSpecService(ManagedChannelBuilder<?> channelBuilder) {
    channel = channelBuilder.usePlaintext(true).build();
    blockingStub = CoreServiceGrpc.newBlockingStub(channel);
  }

  @Override
  public Store getStoreDetails(String id) {
    GetStoresRequest request = GetStoresRequest.newBuilder()
        .setFilter(Filter.newBuilder().setName(id)).build();
    GetStoresResponse response = blockingStub.getStores(request);
    return response.getStoreList()
        .stream()
        .filter(s -> s.getName().equals(id)).findFirst()
        .orElseThrow(() -> new SpecRetrievalException(
            String.format("Unable to find store with name: %s", id)));
  }

  @Override
  public Map<String, FeatureSetSpec> getFeatureSetSpecs(List<Subscription> subscriptions) {
    Map<String, FeatureSetSpec> featureSetSpecMap = new HashMap<>();
    for (Subscription subscription : subscriptions) {
      // Initialise Filter object
      GetFeatureSetsRequest.Filter filter = GetFeatureSetsRequest.Filter.newBuilder()
          .setFeatureSetName(subscription.getName())
          .setFeatureSetVersion(subscription.getVersion()).build();

      // Initialise request
      GetFeatureSetsRequest request = GetFeatureSetsRequest.newBuilder().setFilter(filter).build();

      // Send request
      GetFeatureSetsResponse response = blockingStub.getFeatureSets(request);

      for (FeatureSetSpec featureSetSpec : response.getFeatureSetsList()) {
        featureSetSpecMap
            .put(String.format("%s:%s", featureSetSpec.getName(), featureSetSpec.getVersion()),
                featureSetSpec);
      }
    }
    return featureSetSpecMap;
  }

  /**
   * Check whether connection to core service is ready.
   *
   * @return return true if it is ready. Otherwise, return false.
   */
  public boolean isConnected() {
    ConnectivityState state = channel.getState(true);
    return state == ConnectivityState.IDLE
        || state == ConnectivityState.READY;
  }

  /**
   * Shutdown GRPC channel.
   */
  public void shutdown() throws InterruptedException {
    log.info("Shutting down CoreService");
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }
}
