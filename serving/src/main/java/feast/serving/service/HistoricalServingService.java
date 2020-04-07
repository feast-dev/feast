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
package feast.serving.service;

import feast.serving.ServingAPIProto;
import feast.serving.ServingAPIProto.*;
import feast.serving.ServingAPIProto.Job.Builder;
import feast.serving.specs.CachedSpecService;
import feast.storage.api.retriever.FeatureSetRequest;
import feast.storage.api.retriever.HistoricalRetrievalResult;
import feast.storage.api.retriever.HistoricalRetriever;
import io.grpc.Status;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;

public class HistoricalServingService implements ServingService {

  private static final Logger log =
      org.slf4j.LoggerFactory.getLogger(HistoricalServingService.class);

  private final HistoricalRetriever retriever;
  private final CachedSpecService specService;
  private final JobService jobService;

  public HistoricalServingService(
      HistoricalRetriever retriever, CachedSpecService specService, JobService jobService) {
    this.retriever = retriever;
    this.specService = specService;
    this.jobService = jobService;
  }

  /** {@inheritDoc} */
  @Override
  public GetFeastServingInfoResponse getFeastServingInfo(
      GetFeastServingInfoRequest getFeastServingInfoRequest) {
    return GetFeastServingInfoResponse.newBuilder()
        .setType(FeastServingType.FEAST_SERVING_TYPE_BATCH)
        .setJobStagingLocation(retriever.getStagingLocation())
        .build();
  }

  /** {@inheritDoc} */
  @Override
  public GetOnlineFeaturesResponse getOnlineFeatures(GetOnlineFeaturesRequest getFeaturesRequest) {
    throw Status.UNIMPLEMENTED.withDescription("Method not implemented").asRuntimeException();
  }

  /** {@inheritDoc} */
  @Override
  public GetBatchFeaturesResponse getBatchFeatures(GetBatchFeaturesRequest getFeaturesRequest) {
    List<FeatureSetRequest> featureSetRequests =
        specService.getFeatureSets(getFeaturesRequest.getFeaturesList());
    String retrievalId = UUID.randomUUID().toString();
    Job runningJob =
        Job.newBuilder()
            .setId(retrievalId)
            .setType(JobType.JOB_TYPE_DOWNLOAD)
            .setStatus(JobStatus.JOB_STATUS_RUNNING)
            .build();
    jobService.upsert(runningJob);
    Thread thread =
        new Thread(
            new Runnable() {
              @Override
              public void run() {
                HistoricalRetrievalResult result =
                    retriever.getHistoricalFeatures(
                        retrievalId, getFeaturesRequest.getDatasetSource(), featureSetRequests);
                jobService.upsert(resultToJob(result));
              }
            });
    thread.start();

    return GetBatchFeaturesResponse.newBuilder().setJob(runningJob).build();
  }

  /** {@inheritDoc} */
  @Override
  public GetJobResponse getJob(GetJobRequest getJobRequest) {
    Optional<ServingAPIProto.Job> job = jobService.get(getJobRequest.getJob().getId());
    if (!job.isPresent()) {
      throw Status.NOT_FOUND
          .withDescription(String.format("Job not found: %s", getJobRequest.getJob().getId()))
          .asRuntimeException();
    }
    return GetJobResponse.newBuilder().setJob(job.get()).build();
  }

  private Job resultToJob(HistoricalRetrievalResult result) {
    Builder builder =
        Job.newBuilder()
            .setId(result.getId())
            .setType(JobType.JOB_TYPE_DOWNLOAD)
            .setStatus(result.getStatus());
    if (result.hasError()) {
      return builder.setError(result.getError()).build();
    }
    return builder
        .addAllFileUris(result.getFileUris())
        .setDataFormat(result.getDataFormat())
        .build();
  }
}
