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
package feast.serving.controller;

import static org.mockito.MockitoAnnotations.initMocks;

import com.google.protobuf.Timestamp;
import feast.serving.ServingAPIProto.FeatureReference;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest.EntityRow;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.config.FeastProperties;
import feast.serving.service.ServingService;
import feast.types.ValueProto.Value;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.jaegertracing.Configuration;
import io.opentracing.Tracer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

public class ServingServiceGRpcControllerTest {

  @Mock private ServingService mockServingService;

  @Mock private StreamObserver<GetOnlineFeaturesResponse> mockStreamObserver;

  private GetOnlineFeaturesRequest validRequest;

  private ServingServiceGRpcController service;

  @Before
  public void setUp() {
    initMocks(this);

    validRequest =
        GetOnlineFeaturesRequest.newBuilder()
            .addFeatures(
                FeatureReference.newBuilder()
                    .setName("feature1")
                    .setVersion(1)
                    .setProject("project")
                    .build())
            .addFeatures(
                FeatureReference.newBuilder()
                    .setName("feature2")
                    .setVersion(1)
                    .setProject("project")
                    .build())
            .addEntityRows(
                EntityRow.newBuilder()
                    .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100))
                    .putFields("entity1", Value.newBuilder().setInt64Val(1).build())
                    .putFields("entity2", Value.newBuilder().setInt64Val(1).build()))
            .build();

    Tracer tracer = Configuration.fromEnv("dummy").getTracer();
    FeastProperties feastProperties = new FeastProperties();
    service = new ServingServiceGRpcController(mockServingService, feastProperties, tracer);
  }

  @Test
  public void shouldPassValidRequestAsIs() {
    service.getOnlineFeatures(validRequest, mockStreamObserver);
    Mockito.verify(mockServingService).getOnlineFeatures(validRequest);
  }

  @Test
  public void shouldCallOnErrorIfEntityDatasetIsNotSet() {
    GetOnlineFeaturesRequest missingEntityName =
        GetOnlineFeaturesRequest.newBuilder(validRequest).clearEntityRows().build();
    service.getOnlineFeatures(missingEntityName, mockStreamObserver);
    Mockito.verify(mockStreamObserver).onError(Mockito.any(StatusRuntimeException.class));
  }
}
