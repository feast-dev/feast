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

package feast.serving.service;

import com.google.protobuf.Empty;
import feast.core.CoreServiceGrpc;
import feast.core.CoreServiceProto.CoreServiceTypes.GetEntitiesRequest;
import feast.core.CoreServiceProto.CoreServiceTypes.GetEntitiesResponse;
import feast.core.CoreServiceProto.CoreServiceTypes.GetFeaturesRequest;
import feast.core.CoreServiceProto.CoreServiceTypes.GetFeaturesResponse;
import feast.core.CoreServiceProto.CoreServiceTypes.GetStorageRequest;
import feast.core.CoreServiceProto.CoreServiceTypes.GetStorageResponse;
import feast.core.CoreServiceProto.CoreServiceTypes.ListEntitiesResponse;
import feast.core.CoreServiceProto.CoreServiceTypes.ListFeaturesResponse;
import feast.serving.exception.SpecRetrievalException;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.StorageSpecProto.StorageSpec;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Class responsible for retrieving Feature, Entity, and Storage Spec from Feast Core service.
 */
@Slf4j
public class CoreService implements SpecStorage {

  private final ManagedChannel channel;
  private final CoreServiceGrpc.CoreServiceBlockingStub blockingStub;

  public CoreService(String host, int port) {
    this(ManagedChannelBuilder.forAddress(host, port));
  }

  public CoreService(ManagedChannelBuilder<?> channelBuilder) {
    channel = channelBuilder.usePlaintext(true).build();
    blockingStub = CoreServiceGrpc.newBlockingStub(channel);
  }

  /**
   * Get map of entity ID and {@link EntitySpec} from Core API, given a collection of entityId.
   *
   * @param entityIds collection of entityId to retrieve.
   * @return map of entity ID as key and {@link EntitySpec} value.
   * @throws SpecRetrievalException if any error happens during retrieval
   */
  public Map<String, EntitySpec> getEntitySpecs(Iterable<String> entityIds) {
    GetEntitiesRequest request = GetEntitiesRequest.newBuilder().addAllIds(entityIds).build();

    try {
      GetEntitiesResponse response = blockingStub.getEntities(request);
      return response
          .getEntitiesList()
          .stream()
          .collect(Collectors.toMap(EntitySpec::getName, Function.identity()));
    } catch (StatusRuntimeException e) {
      log.error("GRPC error in getEntitySpecs: {}", e.getStatus());
      throw new SpecRetrievalException("Unable to retrieve entity spec", e);
    }
  }

  /**
   * Get all {@link EntitySpec} from Core API.
   *
   * @return map of entity id as key and {@link EntitySpec} as value.
   */
  public Map<String, EntitySpec> getAllEntitySpecs() {
    try {
      ListEntitiesResponse response = blockingStub.listEntities(Empty.getDefaultInstance());
      return response
          .getEntitiesList()
          .stream()
          .collect(Collectors.toMap(EntitySpec::getName, Function.identity()));
    } catch (StatusRuntimeException e) {
      log.error("GRPC error in getAllEntitySpecs: {}", e.getStatus());
      throw new SpecRetrievalException("Unable to retrieve entity spec", e);
    }
  }

  /**
   * Get map of {@link FeatureSpec} from Core API, given a collection of featureId.
   *
   * @param featureIds collection of entityId to retrieve.
   * @return collection of {@link FeatureSpec}
   * @throws SpecRetrievalException if any error happens during retrieval
   */
  public Map<String, FeatureSpec> getFeatureSpecs(Iterable<String> featureIds) {
    try {
      GetFeaturesRequest request = GetFeaturesRequest.newBuilder().addAllIds(featureIds).build();
      GetFeaturesResponse response = blockingStub.getFeatures(request);
      return response
          .getFeaturesList()
          .stream()
          .collect(Collectors.toMap(FeatureSpec::getId, Function.identity()));
    } catch (StatusRuntimeException e) {
      log.error("GRPC error in getFeatureSpecs: {}", e.getStatus());
      throw new SpecRetrievalException("Unable to retrieve feature specs", e);
    }
  }

  @Override
  public StorageSpec getServingStorageSpec() {
    try {
      GetStorageRequest request = GetStorageRequest.newBuilder()
          .addIds(FeastServing.SERVING_STORAGE_ID).build();
      GetStorageResponse response = blockingStub.getStorage(request);
      return response.getStorageSpecs(0);
    } catch (StatusRuntimeException e) {
      log.error("GRPC error in getServingStorageSpec: {}", e.getStatus());
      throw new SpecRetrievalException("Unable to retrieve storage spec", e);
    }
  }

  /**
   * Get all {@link FeatureSpec} available in Core API.
   *
   * @return map of feature id as key and {@link FeatureSpec} as value.
   */
  public Map<String, FeatureSpec> getAllFeatureSpecs() {
    try {
      ListFeaturesResponse response = blockingStub.listFeatures(Empty.getDefaultInstance());
      return response
          .getFeaturesList()
          .stream()
          .collect(Collectors.toMap(FeatureSpec::getId, Function.identity()));
    } catch (StatusRuntimeException e) {
      log.error("GRPC error in getAllFeatureSpecs, {}", e.getStatus());
      throw new SpecRetrievalException("Unable to retrieve feature specs", e);
    }
  }

  /**
   * Check whether connection to core service is ready.
   *
   * @return return true if it is ready. Otherwise, return false.
   */
  public boolean isConnected() {
    ConnectivityState state = channel.getState(true);
    return state.compareTo(ConnectivityState.READY) == 0;
  }

  /**
   * Shutdown GRPC channel.
   */
  public void shutdown() throws InterruptedException {
    log.info("Shutting down CoreService");
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }
}
