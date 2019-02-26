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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

import com.timgroup.statsd.StatsDClient;
import feast.serving.ServingAPIProto.QueryFeaturesRequest;
import feast.serving.ServingAPIProto.QueryFeaturesResponse;
import feast.serving.service.FeastServing;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.jaegertracing.Configuration;
import io.opentracing.Tracer;
import java.util.Arrays;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ServingGrpcServiceTest {

  @Mock private FeastServing mockFeast;

  @Mock private StreamObserver<QueryFeaturesResponse> mockStreamObserver;

  @Mock private StatsDClient statsDClient;

  private QueryFeaturesRequest validRequest;

  private ServingGrpcService service;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    validRequest =
        QueryFeaturesRequest.newBuilder()
            .setEntityName("driver")
            .addAllEntityId(Arrays.asList("driver1", "driver2", "driver3"))
            .addAllFeatureId(
                Arrays.asList("driver.day.completed_booking", "driver.none.last_opportunity"))
            .build();

    Tracer tracer = Configuration.fromEnv("dummy").getTracer();
    service = new ServingGrpcService(mockFeast, tracer, statsDClient);
  }

  @Test
  public void shouldPassValidRequestAsIs() {
    service.queryFeatures(validRequest, mockStreamObserver);
    verify(mockFeast).queryFeatures(validRequest);
  }

  @Test
  public void shouldCallOnErrorIfEntityNameIsNotSet() {
    QueryFeaturesRequest missingEntityName =
        QueryFeaturesRequest.newBuilder(validRequest).clearEntityName().build();

    service.queryFeatures(missingEntityName, mockStreamObserver);

    verify(mockStreamObserver).onError(any(StatusRuntimeException.class));
  }

  @Test
  public void shouldCallOnErrorIfEntityIdsIsNotSet() {
    QueryFeaturesRequest missingEntityIds =
        QueryFeaturesRequest.newBuilder(validRequest).clearEntityId().build();

    service.queryFeatures(missingEntityIds, mockStreamObserver);

    verify(mockStreamObserver).onError(any(StatusRuntimeException.class));
  }

  @Test
  public void shouldCallOnErrorIfFeatureIdsIsNotSet() {
    QueryFeaturesRequest missingRequestDetails =
        QueryFeaturesRequest.newBuilder(validRequest).clearFeatureId().build();

    service.queryFeatures(missingRequestDetails, mockStreamObserver);

    verify(mockStreamObserver).onError(any(StatusRuntimeException.class));
  }

  @Test
  public void shouldCallOnErrorIfFeatureIdsContainsDifferentEntity() {
    QueryFeaturesRequest differentEntityReq =
        QueryFeaturesRequest.newBuilder(validRequest)
            .addFeatureId("customer.day.order_made")
            .build();

    service.queryFeatures(differentEntityReq, mockStreamObserver);

    verify(mockStreamObserver).onError(any(StatusRuntimeException.class));
  }
}
