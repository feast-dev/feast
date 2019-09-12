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

package feast.serving.grpc;

import com.timgroup.statsd.StatsDClient;
import feast.serving.ServingAPIProto.GetFeaturesRequest;
import feast.serving.ServingAPIProto.GetFeaturesRequest.EntityDataSet;
import feast.serving.ServingAPIProto.GetFeaturesRequest.EntityDataSetRow;
import feast.serving.ServingAPIProto.GetFeaturesRequest.FeatureSet;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.service.FeastServing;
import feast.types.ValueProto.Value;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.jaegertracing.Configuration;
import io.opentracing.Tracer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class ServingGrpcServiceTest {

  private static final String FEATURE_SET_NAME = "feature_set_1";
  private static final String FEATURE_SET_VER = "1";
  private static final String FN_TIMESTAMP = "timestamp";
  private static final String FN_REGION = "region";
  private static final String FN_DRIVER_ID = "driver_id";
  private static final String FN_FEATURE_1 = "feature_1";

  private static final long FN_TIMESTAMP_VAL = System.currentTimeMillis();
  private static final String FN_REGION_VAL = "id";
  private static final String FN_DRIVER_ID_VAL = "100";
  private static final int FN_FEATURE_1_VAL = 10;

  @Mock
  private FeastServing mockFeastServing;

  @Mock
  private StreamObserver<GetOnlineFeaturesResponse> mockStreamObserver;

  @Mock
  private StatsDClient statsDClient;

  private GetFeaturesRequest validRequest;

  private ServingGrpcService service;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    EntityDataSet entityDataSet = getEntityDataSetBuilder(getEntityDataSetRowBuilder()).build();
    validRequest = GetFeaturesRequest.newBuilder().addFeatureSets(getFeatureSet())
        .setEntityDataSet(entityDataSet).build();

    Tracer tracer = Configuration.fromEnv("dummy").getTracer();
    service = new ServingGrpcService(mockFeastServing, tracer, statsDClient);
  }

  @Test
  public void shouldPassValidRequestAsIs() {
    service.getOnlineFeatures(validRequest, mockStreamObserver);
    Mockito.verify(mockFeastServing).getOnlineFeatures(validRequest);
  }

  @Test
  public void shouldCallOnErrorIfEntityDataSetIsNotSet() {
    GetFeaturesRequest missingEntityName =
        GetFeaturesRequest.newBuilder(validRequest).clearEntityDataSet().build();
    service.getOnlineFeatures(missingEntityName, mockStreamObserver);
    Mockito.verify(mockStreamObserver).onError(Mockito.any(StatusRuntimeException.class));
  }

  @Test
  public void shouldCallOnErrorIfEntityDataSetRowAndFieldNameSizeMismatch() {
    // Adding an additional feature value
    EntityDataSet sizeMismatchEntityDataSet = EntityDataSet
        .newBuilder(validRequest.getEntityDataSet()).addFieldNames("some_random_field_name")
        .build();
    GetFeaturesRequest sizeMismatch = GetFeaturesRequest.newBuilder(validRequest)
        .setEntityDataSet(sizeMismatchEntityDataSet).build();
    service.getOnlineFeatures(sizeMismatch, mockStreamObserver);
    Mockito.verify(mockStreamObserver).onError(Mockito.any(StatusRuntimeException.class));
  }

//  @Test
//  public void shouldPassValidRequestAsIs() {
//    service.queryFeatures(validRequest, mockStreamObserver);
//    verify(mockFeast).queryFeatures(validRequest);
//  }
//
//  @Test
//  public void shouldCallOnErrorIfEntityNameIsNotSet() {
//    QueryFeaturesRequest missingEntityName =
//        QueryFeaturesRequest.newBuilder(validRequest).clearEntityName().build();
//
//    service.queryFeatures(missingEntityName, mockStreamObserver);
//
//    verify(mockStreamObserver).onError(any(StatusRuntimeException.class));
//  }
//
//  @Test
//  public void shouldCallOnErrorIfEntityIdsIsNotSet() {
//    QueryFeaturesRequest missingEntityIds =
//        QueryFeaturesRequest.newBuilder(validRequest).clearEntityId().build();
//
//    service.queryFeatures(missingEntityIds, mockStreamObserver);
//
//    verify(mockStreamObserver).onError(any(StatusRuntimeException.class));
//  }
//
//  @Test
//  public void shouldCallOnErrorIfFeatureIdsIsNotSet() {
//    QueryFeaturesRequest missingRequestDetails =
//        QueryFeaturesRequest.newBuilder(validRequest).clearFeatureId().build();
//
//    service.queryFeatures(missingRequestDetails, mockStreamObserver);
//
//    verify(mockStreamObserver).onError(any(StatusRuntimeException.class));
//  }
//
//  @Test
//  public void shouldCallOnErrorIfFeatureIdsContainsDifferentEntity() {
//    QueryFeaturesRequest differentEntityReq =
//        QueryFeaturesRequest.newBuilder(validRequest)
//            .addFeatureId("customer.order_made")
//            .build();
//
//    service.queryFeatures(differentEntityReq, mockStreamObserver);
//
//    verify(mockStreamObserver).onError(any(StatusRuntimeException.class));
//  }

  private FeatureSet getFeatureSet() {
    return FeatureSet.newBuilder().setName(FEATURE_SET_NAME)
        .setVersion(FEATURE_SET_VER).addFeatureNames(FN_FEATURE_1).build();
  }

  private EntityDataSet.Builder getEntityDataSetBuilder(
      EntityDataSetRow.Builder entityDataSetRowBuilder) {
    return EntityDataSet.newBuilder()
        .addFieldNames(FN_REGION)
        .addFieldNames(FN_DRIVER_ID)
        .addEntityDataSetRows(entityDataSetRowBuilder);
  }

  private EntityDataSetRow.Builder getEntityDataSetRowBuilder() {
    return EntityDataSetRow.newBuilder()
        .addValue(Value.newBuilder().setInt64Val(System.currentTimeMillis()))
        .addValue(Value.newBuilder().setStringVal(FN_REGION_VAL))
        .addValue(Value.newBuilder().setStringVal(FN_DRIVER_ID_VAL));
  }

}
