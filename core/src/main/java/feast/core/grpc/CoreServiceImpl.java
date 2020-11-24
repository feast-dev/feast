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
package feast.core.grpc;

import feast.common.auth.service.AuthorizationService;
import feast.common.logging.interceptors.GrpcMessageInterceptor;
import feast.core.config.FeastProperties;
import feast.core.exception.RetrievalException;
import feast.core.grpc.interceptors.MonitoringInterceptor;
import feast.core.model.Project;
import feast.core.service.ProjectService;
import feast.core.service.SpecService;
import feast.proto.core.CoreServiceGrpc.CoreServiceImplBase;
import feast.proto.core.CoreServiceProto.*;
import feast.proto.core.EntityProto.EntitySpecV2;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.context.SecurityContextHolder;

/** Implementation of the feast core GRPC service. */
@Slf4j
@GrpcService(interceptors = {GrpcMessageInterceptor.class, MonitoringInterceptor.class})
public class CoreServiceImpl extends CoreServiceImplBase {

  private final FeastProperties feastProperties;
  private SpecService specService;
  private ProjectService projectService;
  private final AuthorizationService authorizationService;

  @Autowired
  public CoreServiceImpl(
      SpecService specService,
      ProjectService projectService,
      FeastProperties feastProperties,
      AuthorizationService authorizationService) {
    this.specService = specService;
    this.projectService = projectService;
    this.feastProperties = feastProperties;
    this.authorizationService = authorizationService;
  }

  @Override
  public void getFeastCoreVersion(
      GetFeastCoreVersionRequest request,
      StreamObserver<GetFeastCoreVersionResponse> responseObserver) {
    try {
      GetFeastCoreVersionResponse response =
          GetFeastCoreVersionResponse.newBuilder().setVersion(feastProperties.getVersion()).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (RetrievalException | StatusRuntimeException e) {
      log.error("Could not determine Feast Core version: ", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  @Override
  public void getEntity(
      GetEntityRequest request, StreamObserver<GetEntityResponse> responseObserver) {
    try {
      GetEntityResponse response = specService.getEntity(request);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (RetrievalException e) {
      log.error("Unable to fetch entity requested in GetEntity method: ", e);
      responseObserver.onError(
          Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    } catch (IllegalArgumentException e) {
      log.error("Illegal arguments provided to GetEntity method: ", e);
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription(e.getMessage())
              .withCause(e)
              .asRuntimeException());
    } catch (Exception e) {
      log.error("Exception has occurred in GetEntity method: ", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  /** Retrieve a list of features */
  @Override
  public void listFeatures(
      ListFeaturesRequest request, StreamObserver<ListFeaturesResponse> responseObserver) {
    try {
      ListFeaturesResponse response = specService.listFeatures(request.getFilter());
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (IllegalArgumentException e) {
      log.error("Illegal arguments provided to ListFeatures method: ", e);
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription(e.getMessage())
              .withCause(e)
              .asRuntimeException());
    } catch (RetrievalException e) {
      log.error("Unable to fetch entities requested in ListFeatures method: ", e);
      responseObserver.onError(
          Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    } catch (Exception e) {
      log.error("Exception has occurred in ListFeatures method: ", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  /** Retrieve a list of entities */
  @Override
  public void listEntities(
      ListEntitiesRequest request, StreamObserver<ListEntitiesResponse> responseObserver) {
    try {
      ListEntitiesResponse response = specService.listEntities(request.getFilter());
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (IllegalArgumentException e) {
      log.error("Illegal arguments provided to ListEntities method: ", e);
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription(e.getMessage())
              .withCause(e)
              .asRuntimeException());
    } catch (RetrievalException e) {
      log.error("Unable to fetch entities requested in ListEntities method: ", e);
      responseObserver.onError(
          Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    } catch (Exception e) {
      log.error("Exception has occurred in ListEntities method: ", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  @Override
  public void listStores(
      ListStoresRequest request, StreamObserver<ListStoresResponse> responseObserver) {
    try {
      ListStoresResponse response = specService.listStores(request.getFilter());
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (RetrievalException e) {
      log.error("Exception has occurred in ListStores method: ", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  /* Registers an entity to Feast Core */
  @Override
  public void applyEntity(
      ApplyEntityRequest request, StreamObserver<ApplyEntityResponse> responseObserver) {

    String projectId = null;

    try {
      EntitySpecV2 spec = request.getSpec();
      projectId = request.getProject();
      authorizationService.authorizeRequest(SecurityContextHolder.getContext(), projectId);
      ApplyEntityResponse response = specService.applyEntity(spec, projectId);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (org.hibernate.exception.ConstraintViolationException e) {
      log.error(
          "Unable to persist this entity due to a constraint violation. Please ensure that"
              + " field names are unique within the project namespace: ",
          e);
      responseObserver.onError(
          Status.ALREADY_EXISTS.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    } catch (AccessDeniedException e) {
      log.info(String.format("User prevented from accessing project: %s", projectId));
      responseObserver.onError(
          Status.PERMISSION_DENIED
              .withDescription(e.getMessage())
              .withCause(e)
              .asRuntimeException());
    } catch (Exception e) {
      log.error("Exception has occurred in ApplyEntity method: ", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  @Override
  public void updateStore(
      UpdateStoreRequest request, StreamObserver<UpdateStoreResponse> responseObserver) {
    try {
      UpdateStoreResponse response = specService.updateStore(request);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Exception has occurred in UpdateStore method: ", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  @Override
  public void createProject(
      CreateProjectRequest request, StreamObserver<CreateProjectResponse> responseObserver) {
    try {
      projectService.createProject(request.getName());
      responseObserver.onNext(CreateProjectResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Exception has occurred in the createProject method: ", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  @Override
  public void archiveProject(
      ArchiveProjectRequest request, StreamObserver<ArchiveProjectResponse> responseObserver) {
    String projectId = null;
    try {
      projectId = request.getName();
      authorizationService.authorizeRequest(SecurityContextHolder.getContext(), projectId);
      projectService.archiveProject(projectId);
      responseObserver.onNext(ArchiveProjectResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (IllegalArgumentException e) {
      log.error("Recieved an invalid request on calling archiveProject method:", e);
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription(e.getMessage())
              .withCause(e)
              .asRuntimeException());
    } catch (UnsupportedOperationException e) {
      log.error("Attempted to archive an unsupported project:", e);
      responseObserver.onError(
          Status.UNIMPLEMENTED.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    } catch (AccessDeniedException e) {
      log.info(String.format("User prevented from accessing project: %s", projectId));
      responseObserver.onError(
          Status.PERMISSION_DENIED
              .withDescription(e.getMessage())
              .withCause(e)
              .asRuntimeException());
    } catch (Exception e) {
      log.error("Exception has occurred in the createProject method: ", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  @Override
  public void listProjects(
      ListProjectsRequest request, StreamObserver<ListProjectsResponse> responseObserver) {
    try {
      List<Project> projects = projectService.listProjects();
      responseObserver.onNext(
          ListProjectsResponse.newBuilder()
              .addAllProjects(projects.stream().map(Project::getName).collect(Collectors.toList()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Exception has occurred in the listProjects method: ", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  @Override
  public void applyFeatureTable(
      ApplyFeatureTableRequest request,
      StreamObserver<ApplyFeatureTableResponse> responseObserver) {
    String projectName = SpecService.resolveProjectName(request.getProject());
    String tableName = request.getTableSpec().getName();

    try {
      // Check if user has authorization to apply feature table
      authorizationService.authorizeRequest(SecurityContextHolder.getContext(), projectName);

      ApplyFeatureTableResponse response = specService.applyFeatureTable(request);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (AccessDeniedException e) {
      log.info(
          String.format(
              "ApplyFeatureTable: Not authorized to access project to apply: %s", projectName));
      responseObserver.onError(
          Status.PERMISSION_DENIED
              .withDescription(e.getMessage())
              .withCause(e)
              .asRuntimeException());
    } catch (org.hibernate.exception.ConstraintViolationException e) {
      log.error(
          String.format(
              "ApplyFeatureTable: Unable to apply Feature Table due to a conflict: "
                  + "Ensure that name is unique within Project: (name: %s, project: %s)",
              projectName, tableName));
      responseObserver.onError(
          Status.ALREADY_EXISTS.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    } catch (IllegalArgumentException e) {
      log.error(
          String.format(
              "ApplyFeatureTable: Invalid apply Feature Table Request: (name: %s, project: %s)",
              projectName, tableName));
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription(e.getMessage())
              .withCause(e)
              .asRuntimeException());
    } catch (UnsupportedOperationException e) {
      log.error(
          String.format(
              "ApplyFeatureTable: Unsupported apply Feature Table Request: (name: %s, project: %s)",
              projectName, tableName));
      responseObserver.onError(
          Status.UNIMPLEMENTED.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    } catch (Exception e) {
      log.error("ApplyFeatureTable Exception has occurred:", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  @Override
  public void listFeatureTables(
      ListFeatureTablesRequest request,
      StreamObserver<ListFeatureTablesResponse> responseObserver) {
    try {
      ListFeatureTablesResponse response = specService.listFeatureTables(request.getFilter());
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (IllegalArgumentException e) {
      log.error(String.format("ListFeatureTable: Invalid list Feature Table Request"));
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription(e.getMessage())
              .withCause(e)
              .asRuntimeException());
    } catch (Exception e) {
      log.error("ListFeatureTable: Exception has occurred: ", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  @Override
  public void getFeatureTable(
      GetFeatureTableRequest request, StreamObserver<GetFeatureTableResponse> responseObserver) {
    try {
      GetFeatureTableResponse response = specService.getFeatureTable(request);

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (NoSuchElementException e) {
      log.error(
          String.format(
              "GetFeatureTable: No such Feature Table: (project: %s, name: %s)",
              request.getProject(), request.getName()));
      responseObserver.onError(
          Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    } catch (Exception e) {
      log.error("GetFeatureTable: Exception has occurred: ", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  @Override
  public void deleteFeatureTable(
      DeleteFeatureTableRequest request,
      StreamObserver<DeleteFeatureTableResponse> responseObserver) {
    String projectName = request.getProject();
    try {
      // Check if user has authorization to delete feature table
      authorizationService.authorizeRequest(SecurityContextHolder.getContext(), projectName);
      specService.deleteFeatureTable(request);

      responseObserver.onNext(DeleteFeatureTableResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (NoSuchElementException e) {
      log.error(
          String.format(
              "DeleteFeatureTable: No such Feature Table: (project: %s, name: %s)",
              request.getProject(), request.getName()));
      responseObserver.onError(
          Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    } catch (Exception e) {
      log.error("DeleteFeatureTable: Exception has occurred: ", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }
}
