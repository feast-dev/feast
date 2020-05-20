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
package com.gojek.feast;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;

import com.google.protobuf.Timestamp;
import feast.proto.serving.ServingAPIProto.FeatureReference;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequest;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequest.EntityRow;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse.FieldValues;
import feast.proto.serving.ServingServiceGrpc.ServingServiceImplBase;
import feast.proto.types.ValueProto.Value;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class FeastClientTest {
  @Rule public GrpcCleanupRule grpcRule;
  private ServingServiceImplBase servingMock =
      mock(
          ServingServiceImplBase.class,
          delegatesTo(
              new ServingServiceImplBase() {
                @Override
                public void getOnlineFeatures(
                    GetOnlineFeaturesRequest request,
                    StreamObserver<GetOnlineFeaturesResponse> responseObserver) {
                  if (!request.equals(FeastClientTest.getFakeRequest())) {
                    responseObserver.onError(Status.UNKNOWN.asRuntimeException());
                  }

                  responseObserver.onNext(FeastClientTest.getFakeResponse());
                  responseObserver.onCompleted();
                }
              }));
  private FeastClient client;

  @Before
  public void setup() throws Exception {
    this.grpcRule = new GrpcCleanupRule();
    // setup fake serving service
    String serverName = InProcessServerBuilder.generateName();
    this.grpcRule.register(
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(this.servingMock)
            .build()
            .start());

    // setup test feast client target
    ManagedChannel channel =
        this.grpcRule.register(
            InProcessChannelBuilder.forName(serverName).directExecutor().build());
    this.client = new FeastClient(channel);
  }

  @Test
  public void shouldGetOnlineFeatures() {
    List<Row> rows =
        this.client.getOnlineFeatures(
            List.of("driver:name", "rating"),
            List.of(
                Row.create().set("driver_id", 1).setEntityTimestamp(Instant.ofEpochSecond(100))),
            "driver_project");

    assertEquals(
        rows.get(0).getFields(),
        Map.of(
            "driver_id", intValue(1),
            "driver:name", strValue("david"),
            "rating", intValue(3)));
  }

  private static GetOnlineFeaturesRequest getFakeRequest() {
    // setup mock serving service stub
    return GetOnlineFeaturesRequest.newBuilder()
        .addFeatures(
            FeatureReference.newBuilder()
                .setProject("driver_project")
                .setFeatureSet("driver")
                .setName("name")
                .build())
        .addFeatures(
            FeatureReference.newBuilder().setProject("driver_project").setName("rating").build())
        .addEntityRows(
            EntityRow.newBuilder()
                .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100))
                .putFields("driver_id", intValue(1)))
        .build();
  }

  private static GetOnlineFeaturesResponse getFakeResponse() {
    return GetOnlineFeaturesResponse.newBuilder()
        .addFieldValues(
            FieldValues.newBuilder()
                .putAllFields(
                    Map.of(
                        "driver_id", intValue(1),
                        "driver:name", strValue("david"),
                        "rating", intValue(3)))
                .build())
        .build();
  }

  private static Value strValue(String val) {
    return Value.newBuilder().setStringVal(val).build();
  }

  private static Value intValue(int val) {
    return Value.newBuilder().setInt32Val(val).build();
  }
}
