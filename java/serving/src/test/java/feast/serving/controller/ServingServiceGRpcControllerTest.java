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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.protobuf.Timestamp;
import feast.common.auth.authorization.AuthorizationProvider;
import feast.common.auth.authorization.AuthorizationResult;
import feast.common.auth.config.SecurityProperties;
import feast.common.auth.config.SecurityProperties.AuthenticationProperties;
import feast.common.auth.config.SecurityProperties.AuthorizationProperties;
import feast.common.auth.service.AuthorizationService;
import feast.proto.serving.ServingAPIProto.FeatureReferenceV2;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequestV2;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.proto.types.ValueProto.Value;
import feast.serving.config.FeastProperties;
import feast.serving.service.ServingServiceV2;
import io.grpc.stub.StreamObserver;
import io.jaegertracing.Configuration;
import io.opentracing.Tracer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

public class ServingServiceGRpcControllerTest {

  @Mock private ServingServiceV2 mockServingServiceV2;

  @Mock private StreamObserver<GetOnlineFeaturesResponse> mockStreamObserver;

  private GetOnlineFeaturesRequestV2 validRequest;

  private ServingServiceGRpcController service;

  @Mock private Authentication authentication;

  @Mock private AuthorizationProvider authProvider;

  @Before
  public void setUp() {
    initMocks(this);

    validRequest =
        GetOnlineFeaturesRequestV2.newBuilder()
            .addFeatures(
                FeatureReferenceV2.newBuilder()
                    .setFeatureTable("featuretable_1")
                    .setName("feature1")
                    .build())
            .addFeatures(
                FeatureReferenceV2.newBuilder()
                    .setFeatureTable("featuretable_1")
                    .setName("feature2")
                    .build())
            .addEntityRows(
                EntityRow.newBuilder()
                    .setTimestamp(Timestamp.newBuilder().setSeconds(100))
                    .putFields("entity1", Value.newBuilder().setInt64Val(1).build())
                    .putFields("entity2", Value.newBuilder().setInt64Val(1).build()))
            .build();
  }

  private ServingServiceGRpcController getServingServiceGRpcController(boolean enableAuth) {
    Tracer tracer = Configuration.fromEnv("dummy").getTracer();
    FeastProperties feastProperties = new FeastProperties();

    AuthorizationProperties authorizationProps = new AuthorizationProperties();
    authorizationProps.setEnabled(enableAuth);
    AuthenticationProperties authenticationProps = new AuthenticationProperties();
    authenticationProps.setEnabled(enableAuth);
    SecurityProperties securityProperties = new SecurityProperties();
    securityProperties.setAuthentication(authenticationProps);
    securityProperties.setAuthorization(authorizationProps);
    feastProperties.setSecurity(securityProperties);
    AuthorizationService authorizationservice =
        new AuthorizationService(feastProperties.getSecurity(), authProvider);
    return new ServingServiceGRpcController(
        authorizationservice, mockServingServiceV2, feastProperties, tracer);
  }

  @Test
  public void shouldPassValidRequestAsIs() {
    service = getServingServiceGRpcController(false);
    service.getOnlineFeaturesV2(validRequest, mockStreamObserver);
    Mockito.verify(mockServingServiceV2).getOnlineFeatures(validRequest);
  }

  @Test
  public void shouldCallOnErrorIfEntityDatasetIsNotSet() {
    service = getServingServiceGRpcController(false);
    GetOnlineFeaturesRequestV2 missingEntityName =
        GetOnlineFeaturesRequestV2.newBuilder(validRequest).clearEntityRows().build();
    service.getOnlineFeaturesV2(missingEntityName, mockStreamObserver);
    Mockito.verify(mockStreamObserver).onError(Mockito.any(IllegalArgumentException.class));
  }

  @Test
  public void shouldPassValidRequestAsIsIfRequestIsAuthorized() {
    service = getServingServiceGRpcController(true);
    SecurityContext context = mock(SecurityContext.class);
    SecurityContextHolder.setContext(context);
    when(context.getAuthentication()).thenReturn(authentication);
    doReturn(AuthorizationResult.success())
        .when(authProvider)
        .checkAccessToProject(anyString(), any(Authentication.class));
    service.getOnlineFeaturesV2(validRequest, mockStreamObserver);
    Mockito.verify(mockServingServiceV2).getOnlineFeatures(validRequest);
  }
}
