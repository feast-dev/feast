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
import feast.common.auth.credentials.JwtCallCredentials;
import feast.proto.serving.ServingAPIProto.FeatureReferenceV2;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequestV2;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse.FieldStatus;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse.FieldValues;
import feast.proto.serving.ServingServiceGrpc.ServingServiceImplBase;
import feast.proto.types.ValueProto.Value;
import io.grpc.*;
import io.grpc.ServerCall.Listener;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class FeastClientTest {
  private final String AUTH_TOKEN = "test token";

  @Rule public GrpcCleanupRule grpcRule;
  private AtomicBoolean isAuthenticated;

  private ServingServiceImplBase servingMock =
      mock(
          ServingServiceImplBase.class,
          delegatesTo(
              new ServingServiceImplBase() {
                @Override
                public void getOnlineFeaturesV2(
                    GetOnlineFeaturesRequestV2 request,
                    StreamObserver<GetOnlineFeaturesResponse> responseObserver) {
                  if (!request.equals(FeastClientTest.getFakeRequest())) {
                    responseObserver.onError(Status.FAILED_PRECONDITION.asRuntimeException());
                  }

                  responseObserver.onNext(FeastClientTest.getFakeResponse());
                  responseObserver.onCompleted();
                }
              }));

  // Mock Authentication interceptor will flag authenticated request by setting isAuthenticated to
  // true.
  private ServerInterceptor mockAuthInterceptor =
      new ServerInterceptor() {
        @Override
        public <ReqT, RespT> Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
          final Metadata.Key<String> authorizationKey =
              Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER);
          if (headers.containsKey(authorizationKey)) {
            isAuthenticated.set(true);
          }
          return next.startCall(call, headers);
        }
      };

  private FeastClient client;
  private FeastClient authenticatedClient;

  @Before
  public void setup() throws Exception {
    this.grpcRule = new GrpcCleanupRule();
    this.isAuthenticated = new AtomicBoolean(false);
    // setup fake serving service
    String serverName = InProcessServerBuilder.generateName();
    this.grpcRule.register(
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(this.servingMock)
            .intercept(mockAuthInterceptor)
            .build()
            .start());

    // setup test feast client target
    ManagedChannel channel =
        this.grpcRule.register(
            InProcessChannelBuilder.forName(serverName).directExecutor().build());
    this.client = new FeastClient(channel, Optional.empty());
    this.authenticatedClient =
        new FeastClient(channel, Optional.of(new JwtCallCredentials(AUTH_TOKEN)));
  }

  @Test
  public void shouldGetOnlineFeatures() {
    shouldGetOnlineFeaturesWithClient(this.client);
  }

  @Test
  public void shouldAuthenticateAndGetOnlineFeatures() {
    isAuthenticated.set(false);
    shouldGetOnlineFeaturesWithClient(this.authenticatedClient);
    assertEquals(isAuthenticated.get(), true);
  }

  private void shouldGetOnlineFeaturesWithClient(FeastClient client) {
    List<Row> rows =
        client.getOnlineFeatures(
            Arrays.asList("driver:name", "driver:rating", "driver:null_value"),
            Arrays.asList(
                Row.create().set("driver_id", 1).setEntityTimestamp(Instant.ofEpochSecond(100))),
            "driver_project");

    assertEquals(
        rows.get(0).getFields(),
        new HashMap<String, Value>() {
          {
            put("driver_id", intValue(1));
            put("driver:name", strValue("david"));
            put("driver:rating", intValue(3));
            put("driver:null_value", Value.newBuilder().build());
          }
        });
    assertEquals(
        rows.get(0).getStatuses(),
        new HashMap<String, FieldStatus>() {
          {
            put("driver_id", FieldStatus.PRESENT);
            put("driver:name", FieldStatus.PRESENT);
            put("driver:rating", FieldStatus.PRESENT);
            put("driver:null_value", FieldStatus.NULL_VALUE);
          }
        });
  }

  private static GetOnlineFeaturesRequestV2 getFakeRequest() {
    // setup mock serving service stub
    return GetOnlineFeaturesRequestV2.newBuilder()
        .addFeatures(
            FeatureReferenceV2.newBuilder().setFeatureTable("driver").setName("name").build())
        .addFeatures(
            FeatureReferenceV2.newBuilder().setFeatureTable("driver").setName("rating").build())
        .addFeatures(
            FeatureReferenceV2.newBuilder().setFeatureTable("driver").setName("null_value").build())
        .addEntityRows(
            EntityRow.newBuilder()
                .setTimestamp(Timestamp.newBuilder().setSeconds(100))
                .putFields("driver_id", intValue(1)))
        .setProject("driver_project")
        .build();
  }

  private static GetOnlineFeaturesResponse getFakeResponse() {
    return GetOnlineFeaturesResponse.newBuilder()
        .addFieldValues(
            FieldValues.newBuilder()
                .putFields("driver_id", intValue(1))
                .putStatuses("driver_id", FieldStatus.PRESENT)
                .putFields("driver:name", strValue("david"))
                .putStatuses("driver:name", FieldStatus.PRESENT)
                .putFields("driver:rating", intValue(3))
                .putStatuses("driver:rating", FieldStatus.PRESENT)
                .putFields("driver:null_value", Value.newBuilder().build())
                .putStatuses("driver:null_value", FieldStatus.NULL_VALUE)
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
