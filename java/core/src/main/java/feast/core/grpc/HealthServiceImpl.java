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

import feast.core.service.ProjectService;
import io.grpc.Status;
import io.grpc.health.v1.HealthGrpc.HealthImplBase;
import io.grpc.health.v1.HealthProto.HealthCheckRequest;
import io.grpc.health.v1.HealthProto.HealthCheckResponse;
import io.grpc.health.v1.HealthProto.ServingStatus;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
@GrpcService
public class HealthServiceImpl extends HealthImplBase {
  private final ProjectService projectService;

  @Autowired
  public HealthServiceImpl(ProjectService projectService) {
    this.projectService = projectService;
  }

  @Override
  public void check(
      HealthCheckRequest request, StreamObserver<HealthCheckResponse> responseObserver) {
    try {
      projectService.listProjects();
      responseObserver.onNext(
          HealthCheckResponse.newBuilder().setStatus(ServingStatus.SERVING).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Health Check: unable to retrieve projects.\nError: %s", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }
}
