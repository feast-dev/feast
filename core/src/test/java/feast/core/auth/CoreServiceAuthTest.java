/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
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
package feast.core.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import avro.shaded.com.google.common.collect.ImmutableMap;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.common.auth.authorization.AuthorizationProvider;
import feast.common.auth.authorization.AuthorizationResult;
import feast.common.auth.config.SecurityProperties;
import feast.common.auth.service.AuthorizationService;
import feast.common.it.DataGenerator;
import feast.core.config.FeastProperties;
import feast.core.dao.ProjectRepository;
import feast.core.grpc.CoreServiceImpl;
import feast.core.service.ProjectService;
import feast.core.service.SpecService;
import feast.proto.core.CoreServiceProto.ApplyEntityRequest;
import feast.proto.core.CoreServiceProto.ApplyEntityResponse;
import feast.proto.core.EntityProto;
import feast.proto.types.ValueProto;
import io.grpc.internal.testing.StreamRecorder;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

public class CoreServiceAuthTest {

  private CoreServiceImpl coreService;
  private ProjectService projectService;

  @Mock private SpecService specService;
  @Mock private ProjectRepository projectRepository;
  @Mock private AuthorizationProvider authProvider;

  public CoreServiceAuthTest() {
    MockitoAnnotations.initMocks(this);
    SecurityProperties.AuthorizationProperties authProp =
        new SecurityProperties.AuthorizationProperties();
    authProp.setEnabled(true);
    SecurityProperties sp = new SecurityProperties();
    sp.setAuthorization(authProp);
    FeastProperties feastProperties = new FeastProperties();
    feastProperties.setSecurity(sp);
    projectService = new ProjectService(projectRepository);
    AuthorizationService authService =
        new AuthorizationService(feastProperties.getSecurity(), authProvider);
    coreService = new CoreServiceImpl(specService, projectService, feastProperties, authService);
  }

  @Test
  public void shouldNotApplyEntityIfNotProjectMember() throws InvalidProtocolBufferException {

    String project = "project1";
    Authentication auth = mock(Authentication.class);
    SecurityContext context = mock(SecurityContext.class);
    SecurityContextHolder.setContext(context);
    when(context.getAuthentication()).thenReturn(auth);

    doReturn(AuthorizationResult.failed(null))
        .when(authProvider)
        .checkAccessToProject(anyString(), any(Authentication.class));

    StreamRecorder<ApplyEntityResponse> responseObserver = StreamRecorder.create();
    EntityProto.EntitySpecV2 incomingEntitySpec =
        DataGenerator.createEntitySpecV2(
            "entity1",
            "Entity 1 description",
            ValueProto.ValueType.Enum.STRING,
            ImmutableMap.of("label_key", "label_value"));

    ApplyEntityRequest request =
        ApplyEntityRequest.newBuilder().setProject(project).setSpec(incomingEntitySpec).build();

    coreService.applyEntity(request, responseObserver);
    assertEquals("PERMISSION_DENIED: Access Denied", responseObserver.getError().getMessage());
  }

  @Test
  public void shouldApplyEntityIfProjectMember() throws InvalidProtocolBufferException {

    String project = "project1";
    Authentication auth = mock(Authentication.class);
    SecurityContext context = mock(SecurityContext.class);
    SecurityContextHolder.setContext(context);
    when(context.getAuthentication()).thenReturn(auth);
    doReturn(AuthorizationResult.success())
        .when(authProvider)
        .checkAccessToProject(anyString(), any(Authentication.class));

    StreamRecorder<ApplyEntityResponse> responseObserver = StreamRecorder.create();
    EntityProto.EntitySpecV2 incomingEntitySpec =
        DataGenerator.createEntitySpecV2(
            "entity1",
            "Entity 1 description",
            ValueProto.ValueType.Enum.STRING,
            ImmutableMap.of("label_key", "label_value"));
    ApplyEntityRequest request =
        ApplyEntityRequest.newBuilder().setProject(project).setSpec(incomingEntitySpec).build();

    coreService.applyEntity(request, responseObserver);
  }
}
