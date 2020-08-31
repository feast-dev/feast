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
package feast.jobcontroller.grpc;

import com.google.api.gax.rpc.InvalidArgumentException;
import feast.common.logging.interceptors.GrpcMessageInterceptor;
import feast.jobcontroller.service.JobService;
import feast.proto.core.CoreServiceProto.*;
import feast.proto.core.JobControllerServiceGrpc.JobControllerServiceImplBase;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.NoSuchElementException;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.beans.factory.annotation.Autowired;

/** Implementation of the feast core GRPC service. */
@Slf4j
@GrpcService(interceptors = {GrpcMessageInterceptor.class})
public class JobControllerServiceImpl extends JobControllerServiceImplBase {

  private JobService jobService;

  @Autowired
  public JobControllerServiceImpl(JobService jobService) {
    this.jobService = jobService;
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
      log.error("Received an invalid request on calling listIngestionJobs method:", e);
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
