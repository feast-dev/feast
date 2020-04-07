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

import feast.serving.ServingAPIProto.GetBatchFeaturesRequest;
import feast.serving.ServingAPIProto.GetBatchFeaturesResponse;
import feast.serving.ServingAPIProto.GetFeastServingInfoRequest;
import feast.serving.ServingAPIProto.GetFeastServingInfoResponse;
import feast.serving.ServingAPIProto.GetJobRequest;
import feast.serving.ServingAPIProto.GetJobResponse;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;

public interface ServingService {
  /**
   * Get information about the Feast serving deployment.
   *
   * <p>For Bigquery deployments, this includes the default job staging location to load
   * intermediate files to. Otherwise, this method only returns the current Feast Serving backing
   * store type.
   *
   * @param getFeastServingInfoRequest {@link GetFeastServingInfoRequest}
   * @return {@link GetFeastServingInfoResponse}
   */
  GetFeastServingInfoResponse getFeastServingInfo(
      GetFeastServingInfoRequest getFeastServingInfoRequest);

  /**
   * Get features from an online serving store, given a list of {@link
   * feast.serving.ServingAPIProto.FeatureReference}s to retrieve, and list of {@link
   * feast.serving.ServingAPIProto.GetOnlineFeaturesRequest.EntityRow}s to join the retrieved values
   * to.
   *
   * <p>Features can be queried across feature sets, but each {@link
   * feast.serving.ServingAPIProto.GetOnlineFeaturesRequest.EntityRow} must contain all entities for
   * all feature sets included in the request.
   *
   * <p>This request is fulfilled synchronously.
   *
   * @param getFeaturesRequest {@link GetOnlineFeaturesRequest} containing list of {@link
   *     feast.serving.ServingAPIProto.FeatureReference}s to retrieve and list of {@link
   *     feast.serving.ServingAPIProto.GetOnlineFeaturesRequest.EntityRow}s to join the retrieved
   *     values to.
   * @return {@link GetOnlineFeaturesResponse} with list of {@link
   *     feast.serving.ServingAPIProto.GetOnlineFeaturesResponse.FieldValues} for each {@link
   *     feast.serving.ServingAPIProto.GetOnlineFeaturesRequest.EntityRow} supplied.
   */
  GetOnlineFeaturesResponse getOnlineFeatures(GetOnlineFeaturesRequest getFeaturesRequest);

  /**
   * Get features from a batch serving store, given a list of {@link
   * feast.serving.ServingAPIProto.FeatureReference}s to retrieve, and {@link
   * feast.serving.ServingAPIProto.DatasetSource} pointing to remote location of dataset to join
   * retrieved features to. All columns in the provided dataset will be preserved in the output
   * dataset.
   *
   * <p>Due to the potential size of batch retrieval requests, this request is fulfilled
   * asynchronously, and returns a retrieval job id, which when supplied to {@link
   * #getJob(GetJobRequest)} will return the status of the retrieval job.
   *
   * @param getFeaturesRequest {@link GetBatchFeaturesRequest} containing a list of {@link
   *     feast.serving.ServingAPIProto.FeatureReference}s to retrieve, and {@link
   *     feast.serving.ServingAPIProto.DatasetSource} pointing to remote location of dataset to join
   *     retrieved features to.
   * @return {@link GetBatchFeaturesResponse} containing reference to a retrieval {@link
   *     feast.serving.ServingAPIProto.Job}.
   */
  GetBatchFeaturesResponse getBatchFeatures(GetBatchFeaturesRequest getFeaturesRequest);

  /**
   * Get the status of a retrieval job from a batch serving store.
   *
   * <p>The client should check the status of the returned job periodically by calling ReloadJob to
   * determine if the job has completed successfully or with an error. If the job completes
   * successfully i.e. status = JOB_STATUS_DONE with no error, then the client can check the
   * file_uris for the location to download feature values data. The client is assumed to have
   * access to these file URIs.
   *
   * <p>If an error occurred during retrieval, the {@link GetJobResponse} will also contain the
   * error that resulted in termination.
   *
   * @param getJobRequest {@link GetJobRequest} containing reference to a retrieval job
   * @return {@link GetJobResponse}
   */
  GetJobResponse getJob(GetJobRequest getJobRequest);
}
