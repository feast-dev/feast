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
import com.google.protobuf.InvalidProtocolBufferException;
import com.timgroup.statsd.StatsDClient;
import feast.core.CoreServiceGrpc.CoreServiceImplBase;
import feast.core.CoreServiceProto.ApplyFeatureSetRequest;
import feast.core.CoreServiceProto.ApplyFeatureSetResponse;
import feast.core.CoreServiceProto.GetFeastCoreVersionRequest;
import feast.core.CoreServiceProto.GetFeastCoreVersionResponse;
import feast.core.CoreServiceProto.GetFeatureSetsRequest;
import feast.core.CoreServiceProto.GetFeatureSetsResponse;
import feast.core.CoreServiceProto.GetStoresRequest;
import feast.core.CoreServiceProto.GetStoresRequest.Filter;
import feast.core.CoreServiceProto.GetStoresResponse;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.Subscription;
import feast.core.exception.RetrievalException;
import feast.core.service.JobCoordinatorService;
import feast.core.service.SpecService;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

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

  @Override
  public void getFeastCoreVersion(GetFeastCoreVersionRequest request,
      StreamObserver<GetFeastCoreVersionResponse> responseObserver) {
    super.getFeastCoreVersion(request, responseObserver);
  }

  @Override
  @Transactional
  public void getFeatureSets(GetFeatureSetsRequest request,
      StreamObserver<GetFeatureSetsResponse> responseObserver) {
    try {
      GetFeatureSetsResponse response = specService.getFeatureSets(request.getFilter());
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (RetrievalException | InvalidProtocolBufferException e) {
      responseObserver.onError(getRuntimeException(e));
    }
  }

  @Override
  @Transactional
  public void getStores(GetStoresRequest request,
      StreamObserver<GetStoresResponse> responseObserver) {
    try {
      GetStoresResponse response = specService.getStores(request.getFilter());
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (RetrievalException e) {
      responseObserver.onError(getRuntimeException(e));
    }
  }

  @Override
  @Transactional
  public void applyFeatureSet(ApplyFeatureSetRequest request,
      StreamObserver<ApplyFeatureSetResponse> responseObserver) {
    try {
      ApplyFeatureSetResponse response = specService.applyFeatureSet(request.getFeatureSet());
      String featureSetName = response.getFeatureSet().getName();
      GetStoresResponse stores = specService.getStores(Filter.newBuilder().build());
      for (Store store : stores.getStoreList()) {
        List<Subscription> relevantSubscriptions = store.getSubscriptionsList().stream()
            .filter(sub -> {
              Pattern p = Pattern.compile(sub.getName());
              return p.matcher(featureSetName).matches();
            })
            .collect(Collectors.toList());
        List<FeatureSetSpec> featureSetSpecs = new ArrayList<>();
        for (Subscription subscription : relevantSubscriptions) {
          featureSetSpecs.addAll(specService.getFeatureSets(
              GetFeatureSetsRequest.Filter
                  .newBuilder()
                  .setFeatureSetName(featureSetName)
                  .setFeatureSetVersion(subscription.getVersion())
                  .build()
          ).getFeatureSetsList());
        }
        if (!featureSetSpecs.isEmpty()) {
          jobCoordinatorService.startOrUpdateJob(featureSetSpecs, store);
        }
      }
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(getRuntimeException(e));
    }
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

