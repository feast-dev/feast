/*
 * Copyright 2018 The Feast Authors
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
 *
 */

package feast.core.grpc;

import com.google.protobuf.Empty;
import feast.core.JobServiceGrpc;
import feast.core.JobServiceProto.JobServiceTypes.AbortJobRequest;
import feast.core.JobServiceProto.JobServiceTypes.AbortJobResponse;
import feast.core.JobServiceProto.JobServiceTypes.GetJobRequest;
import feast.core.JobServiceProto.JobServiceTypes.GetJobResponse;
import feast.core.JobServiceProto.JobServiceTypes.JobDetail;
import feast.core.JobServiceProto.JobServiceTypes.ListJobsResponse;
import feast.core.JobServiceProto.JobServiceTypes.SubmitImportJobRequest;
import feast.core.JobServiceProto.JobServiceTypes.SubmitImportJobResponse;
import feast.core.exception.JobExecutionException;
import feast.core.service.JobManagementService;
import feast.core.validators.SpecValidator;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;

/** Implementation of the feast job GRPC service. */
@Slf4j
@GRpcService
public class JobServiceImpl extends JobServiceGrpc.JobServiceImplBase {
  @Autowired private JobManagementService jobManagementService;

  @Autowired private SpecValidator validator;

  /**
   * submit a job to the runner by providing an import spec.
   *
   * @param request ImportJobRequest object containing an import spec
   * @param responseObserver
   */
  @Override
  public void submitJob(
      SubmitImportJobRequest request, StreamObserver<SubmitImportJobResponse> responseObserver) {
    try {
      validator.validateImportSpec(request.getImportSpec());
      String jobID = jobManagementService.submitJob(request.getImportSpec(), request.getName());
      SubmitImportJobResponse response =
          SubmitImportJobResponse.newBuilder().setJobId(jobID).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (IllegalArgumentException e) {
      log.error("Error in startJob: {}", e);
      responseObserver.onError(getBadRequestException(e));
    } catch (JobExecutionException e) {
      log.error("Error in startJob: {}", e);
      responseObserver.onError(getRuntimeException(e));
    }
  }

  /**
   * Abort a job given its feast-internal job id
   *
   * @param request AbortJobRequest object containing feast job id
   * @param responseObserver
   */
  @Override
  public void abortJob(AbortJobRequest request, StreamObserver<AbortJobResponse> responseObserver) {
    try {
      jobManagementService.abortJob(request.getId());
      AbortJobResponse response = AbortJobResponse.newBuilder().setId(request.getId()).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Error aborting job with id {}: {}", request.getId(), e);
      responseObserver.onError(getRuntimeException(e));
    }
  }

  /**
   * List all jobs previously submitted to the system.
   *
   * @param request Empty request
   * @param responseObserver
   */
  @Override
  public void listJobs(Empty request, StreamObserver<ListJobsResponse> responseObserver) {
    try {
      List<JobDetail> jobs = jobManagementService.listJobs();
      ListJobsResponse response = ListJobsResponse.newBuilder().addAllJobs(jobs).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Error listing jobs: {}", e);
      responseObserver.onError(getRuntimeException(e));
    }
  }

  /**
   * Get a single job previously submitted to the system by id
   *
   * @param request GetJobRequest object containing a feast-internal job id
   * @param responseObserver
   */
  @Override
  public void getJob(GetJobRequest request, StreamObserver<GetJobResponse> responseObserver) {
    try {
      JobDetail job = jobManagementService.getJob(request.getId());
      GetJobResponse response = GetJobResponse.newBuilder().setJob(job).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Error getting job id {}: {}", request.getId(), e);
      responseObserver.onError(getRuntimeException(e));
    }
  }

  private StatusRuntimeException getRuntimeException(Exception e) {
    return new StatusRuntimeException(
        Status.fromCode(Status.Code.INTERNAL).withDescription(e.getMessage()).withCause(e));
  }

  private StatusRuntimeException getBadRequestException(Exception e) {
    return new StatusRuntimeException(
        Status.fromCode(Status.Code.OUT_OF_RANGE).withDescription(e.getMessage()).withCause(e));
  }
}
