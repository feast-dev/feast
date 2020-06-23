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

import com.google.api.gax.rpc.InvalidArgumentException;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.config.FeastProperties;
import feast.core.exception.RetrievalException;
import feast.core.grpc.interceptors.MonitoringInterceptor;
import feast.core.model.Project;
import feast.core.service.AccessManagementService;
import feast.core.service.JobService;
import feast.core.service.SpecService;
import feast.core.service.StatsService;
import feast.proto.core.CoreServiceGrpc.CoreServiceImplBase;
import feast.proto.core.CoreServiceProto.*;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.context.SecurityContextHolder;

/** Implementation of the feast core GRPC service. */
@Slf4j
@GrpcService(interceptors = {MonitoringInterceptor.class})
public class CoreServiceImpl extends CoreServiceImplBase {

  private final FeastProperties feastProperties;
  private SpecService specService;
  private JobService jobService;
  private StatsService statsService;
  private AccessManagementService accessManagementService;

  @Autowired
  public CoreServiceImpl(
      SpecService specService,
      AccessManagementService accessManagementService,
      StatsService statsService,
      JobService jobService,
      FeastProperties feastProperties) {
    this.specService = specService;
    this.accessManagementService = accessManagementService;
    this.jobService = jobService;
    this.feastProperties = feastProperties;
    this.statsService = statsService;
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
  public void getFeatureSet(
      GetFeatureSetRequest request, StreamObserver<GetFeatureSetResponse> responseObserver) {
    try {
      GetFeatureSetResponse response = specService.getFeatureSet(request);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (RetrievalException | StatusRuntimeException | InvalidProtocolBufferException e) {
      log.error("Exception has occurred in GetFeatureSet method: ", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  @Override
  public void listFeatureSets(
      ListFeatureSetsRequest request, StreamObserver<ListFeatureSetsResponse> responseObserver) {
    try {
      ListFeatureSetsResponse response = specService.listFeatureSets(request.getFilter());
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (RetrievalException | IllegalArgumentException | InvalidProtocolBufferException e) {
      log.error("Exception has occurred in ListFeatureSet method: ", e);
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

  @Override
  public void getFeatureStatistics(
      GetFeatureStatisticsRequest request,
      StreamObserver<GetFeatureStatisticsResponse> responseObserver) {
    try {
      GetFeatureStatisticsResponse response = statsService.getFeatureStatistics(request);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (IllegalArgumentException e) {
      log.error("Illegal arguments provided to GetFeatureStatistics method: ", e);
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription(e.getMessage())
              .withCause(e)
              .asRuntimeException());
    } catch (RetrievalException e) {
      log.error("Unable to fetch feature set requested in GetFeatureStatistics method: ", e);
      responseObserver.onError(
          Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    } catch (Exception e) {
      log.error("Exception has occurred in GetFeatureStatistics method: ", e);
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

  @Override
  public void applyFeatureSet(
      ApplyFeatureSetRequest request, StreamObserver<ApplyFeatureSetResponse> responseObserver) {

    accessManagementService.checkIfProjectMember(
        SecurityContextHolder.getContext(), request.getFeatureSet().getSpec().getProject());

    try {
      ApplyFeatureSetResponse response = specService.applyFeatureSet(request.getFeatureSet());
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (org.hibernate.exception.ConstraintViolationException e) {
      log.error(
          "Unable to persist this feature set due to a constraint violation. Please ensure that"
              + " field names are unique within the project namespace: ",
          e);
      responseObserver.onError(
          Status.ALREADY_EXISTS.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    } catch (Exception e) {
      log.error("Exception has occurred in ApplyFeatureSet method: ", e);
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
      accessManagementService.createProject(request.getName());
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

    accessManagementService.checkIfProjectMember(
        SecurityContextHolder.getContext(), request.getName());

    try {
      accessManagementService.archiveProject(request.getName());
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
      List<Project> projects = accessManagementService.listProjects();
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
  public void listIngestionJobs(
      ListIngestionJobsRequest request,
      StreamObserver<ListIngestionJobsResponse> responseObserver) {
    try {
      ListIngestionJobsResponse response = this.jobService.listJobs(request);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (InvalidArgumentException e) {
      log.error("Recieved an invalid request on calling listIngestionJobs method:", e);
      responseObserver.onError(
          Status.INVALID_ARGUMENT.withDescription(e.getMessage()).withCause(e).asException());
    } catch (Exception e) {
      log.error("Unexpected exception on calling listIngestionJobs method:", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  @Override
  public void restartIngestionJob(
      RestartIngestionJobRequest request,
      StreamObserver<RestartIngestionJobResponse> responseObserver) {
    try {
      RestartIngestionJobResponse response = this.jobService.restartJob(request);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (NoSuchElementException e) {
      log.error(
          "Attempted to restart an nonexistent job on calling restartIngestionJob method:", e);
      responseObserver.onError(
          Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asException());
    } catch (UnsupportedOperationException e) {
      log.error("Recieved an unsupported request on calling restartIngestionJob method:", e);
      responseObserver.onError(
          Status.FAILED_PRECONDITION.withDescription(e.getMessage()).withCause(e).asException());
    } catch (Exception e) {
      log.error("Unexpected exception on calling restartIngestionJob method:", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  @Override
  public void stopIngestionJob(
      StopIngestionJobRequest request, StreamObserver<StopIngestionJobResponse> responseObserver) {
    try {
      StopIngestionJobResponse response = this.jobService.stopJob(request);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (NoSuchElementException e) {
      log.error("Attempted to stop an nonexistent job on calling stopIngestionJob method:", e);
      responseObserver.onError(
          Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asException());
    } catch (UnsupportedOperationException e) {
      log.error("Recieved an unsupported request on calling stopIngestionJob method:", e);
      responseObserver.onError(
          Status.FAILED_PRECONDITION.withDescription(e.getMessage()).withCause(e).asException());
    } catch (Exception e) {
      log.error("Unexpected exception on calling stopIngestionJob method:", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }
}
