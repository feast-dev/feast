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
package feast.core.grpc;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.auth.authorization.AuthorizationProvider;
import feast.core.auth.authorization.AuthorizationResult;
import feast.core.config.FeastProperties;
import feast.core.config.FeastProperties.SecurityProperties;
import feast.core.dao.ProjectRepository;
import feast.core.model.Entity;
import feast.core.model.Feature;
import feast.core.model.FeatureSet;
import feast.core.model.Source;
import feast.core.service.AccessManagementService;
import feast.core.service.JobService;
import feast.core.service.SpecService;
import feast.core.service.StatsService;
import feast.proto.core.CoreServiceProto.ApplyFeatureSetRequest;
import feast.proto.core.CoreServiceProto.ApplyFeatureSetResponse;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.FeatureSetProto.FeatureSetStatus;
import feast.proto.core.SourceProto;
import feast.proto.core.SourceProto.KafkaSourceConfig;
import feast.proto.core.SourceProto.SourceType;
import feast.proto.types.ValueProto.ValueType.Enum;
import io.grpc.internal.testing.StreamRecorder;
import java.sql.Date;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

@SpringBootTest
class CoreServiceAuthTest {

  private CoreServiceImpl coreService;
  private AccessManagementService accessManagementService;

  @Mock private SpecService specService;
  @Mock private ProjectRepository projectRepository;
  @Mock private AuthorizationProvider authProvider;
  @Mock private StatsService statsService;
  @Mock private JobService jobService;

  CoreServiceAuthTest() {
    MockitoAnnotations.initMocks(this);
    FeastProperties.SecurityProperties.AuthorizationProperties authProp =
        new FeastProperties.SecurityProperties.AuthorizationProperties();
    authProp.setEnabled(true);
    FeastProperties.SecurityProperties sp = new SecurityProperties();
    sp.setAuthorization(authProp);
    FeastProperties feastProperties = new FeastProperties();
    feastProperties.setSecurity(sp);
    accessManagementService =
        new AccessManagementService(feastProperties, projectRepository, authProvider);
    coreService =
        new CoreServiceImpl(
            specService, accessManagementService, statsService, jobService, feastProperties);
  }

  @Test
  void cantApplyFeatureSetIfNotProjectMember() throws InvalidProtocolBufferException {

    String project = "project1";
    Authentication auth = mock(Authentication.class);
    SecurityContext context = mock(SecurityContext.class);
    SecurityContextHolder.setContext(context);
    when(context.getAuthentication()).thenReturn(auth);

    doReturn(AuthorizationResult.failed(null))
        .when(authProvider)
        .checkAccess(anyString(), any(Authentication.class));

    StreamRecorder<ApplyFeatureSetResponse> responseObserver = StreamRecorder.create();
    FeatureSetProto.FeatureSet incomingFeatureSet = newDummyFeatureSet("f2", 1, project).toProto();
    FeatureSetProto.FeatureSetSpec incomingFeatureSetSpec =
        incomingFeatureSet.getSpec().toBuilder().build();
    FeatureSetProto.FeatureSet spec =
        FeatureSetProto.FeatureSet.newBuilder().setSpec(incomingFeatureSetSpec).build();
    ApplyFeatureSetRequest request =
        ApplyFeatureSetRequest.newBuilder().setFeatureSet(spec).build();

    assertThrows(
        AccessDeniedException.class, () -> coreService.applyFeatureSet(request, responseObserver));
  }

  @Test
  void canApplyFeatureSetIfProjectMember() throws InvalidProtocolBufferException {

    String project = "project1";
    Authentication auth = mock(Authentication.class);
    SecurityContext context = mock(SecurityContext.class);
    SecurityContextHolder.setContext(context);
    when(context.getAuthentication()).thenReturn(auth);
    doReturn(AuthorizationResult.success())
        .when(authProvider)
        .checkAccess(anyString(), any(Authentication.class));

    StreamRecorder<ApplyFeatureSetResponse> responseObserver = StreamRecorder.create();
    FeatureSetProto.FeatureSet incomingFeatureSet = newDummyFeatureSet("f2", 1, project).toProto();
    FeatureSetProto.FeatureSetSpec incomingFeatureSetSpec =
        incomingFeatureSet.getSpec().toBuilder().build();
    FeatureSetProto.FeatureSet spec =
        FeatureSetProto.FeatureSet.newBuilder().setSpec(incomingFeatureSetSpec).build();
    ApplyFeatureSetRequest request =
        ApplyFeatureSetRequest.newBuilder().setFeatureSet(spec).build();

    coreService.applyFeatureSet(request, responseObserver);
  }

  private FeatureSet newDummyFeatureSet(String name, int version, String project) {
    Feature feature = new Feature("feature", Enum.INT64);
    Entity entity = new Entity("entity", Enum.STRING);
    SourceProto.Source sourceSpec =
        SourceProto.Source.newBuilder()
            .setType(SourceType.KAFKA)
            .setKafkaSourceConfig(
                KafkaSourceConfig.newBuilder()
                    .setBootstrapServers("kafka:9092")
                    .setTopic("my-topic")
                    .build())
            .build();
    Source defaultSource = Source.fromProto(sourceSpec);
    FeatureSet fs =
        new FeatureSet(
            name,
            project,
            100L,
            Arrays.asList(entity),
            Arrays.asList(feature),
            defaultSource,
            new HashMap<String, String>(),
            FeatureSetStatus.STATUS_READY);
    fs.setCreated(Date.from(Instant.ofEpochSecond(10L)));
    return fs;
  }
}
