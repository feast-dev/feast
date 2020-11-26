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
package feast.serving.service;

import feast.proto.serving.ServingAPIProto;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequestV2;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse;

public interface ServingServiceV2 {
  /**
   * Get information about the Feast serving deployment.
   *
   * <p>For Bigquery deployments, this includes the default job staging location to load
   * intermediate files to. Otherwise, this method only returns the current Feast Serving backing
   * store type.
   *
   * @param getFeastServingInfoRequest {@link ServingAPIProto.GetFeastServingInfoRequest}
   * @return {@link ServingAPIProto.GetFeastServingInfoResponse}
   */
  ServingAPIProto.GetFeastServingInfoResponse getFeastServingInfo(
      ServingAPIProto.GetFeastServingInfoRequest getFeastServingInfoRequest);

  /**
   * Get features from an online serving store, given a list of {@link
   * feast.proto.serving.ServingAPIProto.FeatureReferenceV2}s to retrieve, and list of {@link
   * feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow}s to join the
   * retrieved values to.
   *
   * <p>Features can be queried across feature tables, but each {@link
   * feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow} must contain all
   * entities for all feature tables included in the request.
   *
   * <p>This request is fulfilled synchronously.
   *
   * @param getFeaturesRequest {@link GetOnlineFeaturesRequestV2} containing list of {@link
   *     feast.proto.serving.ServingAPIProto.FeatureReferenceV2}s to retrieve and list of {@link
   *     feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow}s to join the
   *     retrieved values to.
   * @return {@link GetOnlineFeaturesResponse} with list of {@link
   *     feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse.FieldValues} for each {@link
   *     feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow} supplied.
   */
  GetOnlineFeaturesResponse getOnlineFeatures(GetOnlineFeaturesRequestV2 getFeaturesRequest);
}
