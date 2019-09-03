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

package feast.core.grpc;

import com.google.protobuf.Empty;
import com.timgroup.statsd.StatsDClient;
import feast.core.CoreServiceGrpc.CoreServiceImplBase;
import feast.core.CoreServiceProto.ApplyFeatureSetRequest;
import feast.core.CoreServiceProto.ApplyFeatureSetResponse;
import feast.core.CoreServiceProto.GetFeastCoreVersionResponse;
import feast.core.CoreServiceProto.GetFeatureSetsRequest;
import feast.core.CoreServiceProto.GetFeatureSetsResponse;
import feast.core.CoreServiceProto.GetStoresRequest;
import feast.core.CoreServiceProto.GetStoresResponse;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.StoreProto;
import feast.core.exception.RetrievalException;
import feast.core.model.FeatureSet;
import feast.core.model.Store;
import feast.core.service.FeatureStreamService;
import feast.core.service.JobCoordinatorService;
import feast.core.service.SpecService;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Implementation of the feast core GRPC service.
 */
@Slf4j
@GRpcService
public class CoreServiceImpl extends CoreServiceImplBase {

  @Autowired
  private SpecService specService;
  @Autowired
  private StatsDClient statsDClient;
  @Autowired
  private JobCoordinatorService jobCoordinatorService;
  @Autowired
  private FeatureStreamService featureStreamService;


  @Override
  public void getFeastCoreVersion(Empty request,
      StreamObserver<GetFeastCoreVersionResponse> responseObserver) {
    super.getFeastCoreVersion(request, responseObserver);
  }

  @Override
  public void getFeatureSets(GetFeatureSetsRequest request,
      StreamObserver<GetFeatureSetsResponse> responseObserver) {
    try {
      List<FeatureSet> featureSets = specService.getFeatureSets(request.getFilter());
      List<FeatureSetSpec> featureSetProtos = featureSets.stream()
          .map(FeatureSet::toProto).collect(Collectors.toList());
      GetFeatureSetsResponse response = GetFeatureSetsResponse.newBuilder()
          .addAllFeatureSets(featureSetProtos)
          .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (RetrievalException e) {
      responseObserver.onError(getRuntimeException(e));
    }
  }

  @Override
  public void getStores(GetStoresRequest request,
      StreamObserver<GetStoresResponse> responseObserver) {
    try {
      List<Store> stores = specService.getStores(request.getFilter());
      List<StoreProto.Store> storeProtos = stores.stream()
          .map(Store::toProto).collect(Collectors.toList());
      GetStoresResponse response = GetStoresResponse.newBuilder()
          .addAllStore(storeProtos)
          .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (RetrievalException e) {
      responseObserver.onError(getRuntimeException(e));
    }
  }

  @Override
  public void applyFeatureSet(ApplyFeatureSetRequest request,
      StreamObserver<ApplyFeatureSetResponse> responseObserver) {
    super.applyFeatureSet(request, responseObserver);
  }

  private StatusRuntimeException getRuntimeException(Exception e) {
    return new StatusRuntimeException(
        Status.fromCode(Status.Code.INTERNAL).withDescription(e.getMessage()).withCause(e));
  }

  private StatusRuntimeException getBadRequestException(Exception e) {
    return new StatusRuntimeException(
        Status.fromCode(Status.Code.OUT_OF_RANGE).withDescription(e.getMessage()).withCause(e));
  }
}

