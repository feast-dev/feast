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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.CoreServiceProto.ApplyFeatureSetRequest;
import feast.core.CoreServiceProto.ApplyFeatureSetResponse;
import feast.core.FeatureSetProto;
import feast.core.FeatureSetProto.FeatureSetStatus;
import feast.core.SourceProto.KafkaSourceConfig;
import feast.core.SourceProto.SourceType;
import feast.core.auth.authorization.AuthorizationProvider;
import feast.core.config.FeastProperties;
import feast.core.config.FeastProperties.SecurityProperties;
import feast.core.dao.ProjectRepository;
import feast.core.model.FeatureSet;
import feast.core.model.Field;
import feast.core.model.Source;
import feast.core.service.ProjectService;
import feast.core.service.SpecService;
import feast.types.ValueProto.ValueType.Enum;
import io.grpc.internal.testing.StreamRecorder;
import java.sql.Date;
import java.time.Instant;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

@SpringBootTest
class CoreServiceAuthTest {

  private CoreServiceImpl coreService;
  private SpecService specService;
  private ProjectService projectService;
  private ProjectRepository projectRepository;
  private AuthorizationProvider authProvider;

  CoreServiceAuthTest() {
    specService = mock(SpecService.class);
    projectRepository = mock(ProjectRepository.class);
    authProvider = mock(AuthorizationProvider.class);
    FeastProperties.SecurityProperties.AuthorizationProperties authProp =
        new FeastProperties.SecurityProperties.AuthorizationProperties();
    authProp.setEnabled(true);
    FeastProperties.SecurityProperties sp = new SecurityProperties();
    sp.setAuthorization(authProp);
    FeastProperties feastProperties = new FeastProperties();
    feastProperties.setSecurity(sp);
    projectService = new ProjectService(feastProperties, projectRepository, authProvider);
    coreService = new CoreServiceImpl(specService, projectService);
  }

  @Test
  void cantApplyFeatureSetIfNotProjectMember() throws InvalidProtocolBufferException {

    String project = "project1";
    Authentication auth = mock(Authentication.class);
    SecurityContext context = mock(SecurityContext.class);
    SecurityContextHolder.setContext(context);
    when(context.getAuthentication()).thenReturn(auth);

    doThrow(AccessDeniedException.class).when(authProvider).checkIfProjectMember(project, auth);

    StreamRecorder<ApplyFeatureSetResponse> responseObserver = StreamRecorder.create();
    FeatureSetProto.FeatureSet incomingFeatureSet = newDummyFeatureSet("f2", 1, project).toProto();
    FeatureSetProto.FeatureSetSpec incomingFeatureSetSpec =
        incomingFeatureSet.getSpec().toBuilder().clearVersion().build();
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

    StreamRecorder<ApplyFeatureSetResponse> responseObserver = StreamRecorder.create();
    FeatureSetProto.FeatureSet incomingFeatureSet = newDummyFeatureSet("f2", 1, project).toProto();
    FeatureSetProto.FeatureSetSpec incomingFeatureSetSpec =
        incomingFeatureSet.getSpec().toBuilder().clearVersion().build();
    FeatureSetProto.FeatureSet spec =
        FeatureSetProto.FeatureSet.newBuilder().setSpec(incomingFeatureSetSpec).build();
    ApplyFeatureSetRequest request =
        ApplyFeatureSetRequest.newBuilder().setFeatureSet(spec).build();

    coreService.applyFeatureSet(request, responseObserver);
  }

  private FeatureSet newDummyFeatureSet(String name, int version, String project) {
    Field feature = new Field("feature", Enum.INT64);
    Field entity = new Field("entity", Enum.STRING);

    Source defaultSource =
        new Source(
            SourceType.KAFKA,
            KafkaSourceConfig.newBuilder()
                .setBootstrapServers("kafka:9092")
                .setTopic("my-topic")
                .build(),
            true);

    FeatureSet fs =
        new FeatureSet(
            name,
            project,
            version,
            100L,
            Arrays.asList(entity),
            Arrays.asList(feature),
            defaultSource,
            FeatureSetStatus.STATUS_READY);
    fs.setCreated(Date.from(Instant.ofEpochSecond(10L)));
    return fs;
  }
}
