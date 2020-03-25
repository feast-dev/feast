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
import feast.serving.specs.CachedSpecService;
import feast.storage.api.retrieval.BatchRetriever;
import feast.storage.api.retrieval.FeatureSetRequest;
import feast.storage.connectors.bigquery.retrieval.BigQueryBatchRetriever;
import io.grpc.Status;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;

public class BatchServingService implements ServingService {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(BatchServingService.class);

  private final BatchRetriever retriever;
  private final CachedSpecService specService;
  private final JobService jobService;

  public BatchServingService(
      BatchRetriever retriever, CachedSpecService specService, JobService jobService) {
    this.retriever = retriever;
    this.specService = specService;
    this.jobService = jobService;
  }

  /** {@inheritDoc} */
  @Override
  public GetFeastServingInfoResponse getFeastServingInfo(
      GetFeastServingInfoRequest getFeastServingInfoRequest) {
    try {
      BigQueryBatchRetriever bigQueryBatchRetriever = (BigQueryBatchRetriever) retriever;
      return GetFeastServingInfoResponse.newBuilder()
          .setType(FeastServingType.FEAST_SERVING_TYPE_BATCH)
          .setJobStagingLocation(bigQueryBatchRetriever.jobStagingLocation())
          .build();
    } catch (Exception e) {
      return GetFeastServingInfoResponse.newBuilder()
          .setType(FeastServingType.FEAST_SERVING_TYPE_BATCH)
          .build();
    }
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
    Job feastJob = retriever.getBatchFeatures(getFeaturesRequest, featureSetRequests);
    jobService.upsert(feastJob);
    return GetBatchFeaturesResponse.newBuilder().setJob(feastJob).build();
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
}
