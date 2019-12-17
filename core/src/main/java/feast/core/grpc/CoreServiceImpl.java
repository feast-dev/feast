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
package feast.core.grpc;

import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.CoreServiceGrpc.CoreServiceImplBase;
import feast.core.CoreServiceProto.ApplyFeatureSetRequest;
import feast.core.CoreServiceProto.ApplyFeatureSetResponse;
import feast.core.CoreServiceProto.GetFeastCoreVersionRequest;
import feast.core.CoreServiceProto.GetFeastCoreVersionResponse;
import feast.core.CoreServiceProto.GetFeatureSetRequest;
import feast.core.CoreServiceProto.GetFeatureSetResponse;
import feast.core.CoreServiceProto.ListFeatureSetsRequest;
import feast.core.CoreServiceProto.ListFeatureSetsResponse;
import feast.core.CoreServiceProto.ListStoresRequest;
import feast.core.CoreServiceProto.ListStoresResponse;
import feast.core.CoreServiceProto.UpdateStoreRequest;
import feast.core.CoreServiceProto.UpdateStoreResponse;
import feast.core.exception.RetrievalException;
import feast.core.grpc.interceptors.MonitoringInterceptor;
import feast.core.service.SpecService;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;

/** Implementation of the feast core GRPC service. */
@Slf4j
@GRpcService(interceptors = {MonitoringInterceptor.class})
public class CoreServiceImpl extends CoreServiceImplBase {

  private SpecService specService;

  @Autowired
  public CoreServiceImpl(SpecService specService) {
    this.specService = specService;
  }

  @Override
  public void getFeastCoreVersion(
      GetFeastCoreVersionRequest request,
      StreamObserver<GetFeastCoreVersionResponse> responseObserver) {
    super.getFeastCoreVersion(request, responseObserver);
  }

  @Override
  public void getFeatureSet(
      GetFeatureSetRequest request, StreamObserver<GetFeatureSetResponse> responseObserver) {
    try {
      GetFeatureSetResponse response = specService.getFeatureSet(request);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (RetrievalException | InvalidProtocolBufferException | StatusRuntimeException e) {
      log.error("Exception has occurred in GetFeatureSet method: ", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void listFeatureSets(
      ListFeatureSetsRequest request, StreamObserver<ListFeatureSetsResponse> responseObserver) {
    try {
      ListFeatureSetsResponse response = specService.listFeatureSets(request.getFilter());
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (RetrievalException | InvalidProtocolBufferException e) {
      log.error("Exception has occurred in ListFeatureSet method: ", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void listStores(
      ListStoresRequest request, StreamObserver<ListStoresResponse> responseObserver) {
    try {
      ListStoresResponse response = specService.listStores(request.getFilter());
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (RetrievalException e) {
      log.error("Exception has occurred in ListStores method: ", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void applyFeatureSet(
      ApplyFeatureSetRequest request, StreamObserver<ApplyFeatureSetResponse> responseObserver) {
    try {
      ApplyFeatureSetResponse response = specService.applyFeatureSet(request.getFeatureSet());
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Exception has occurred in ApplyFeatureSet method: ", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateStore(
      UpdateStoreRequest request, StreamObserver<UpdateStoreResponse> responseObserver) {
    try {
      UpdateStoreResponse response = specService.updateStore(request);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Exception has occurred in UpdateStore method: ", e);
      responseObserver.onError(e);
    }
  }
}
