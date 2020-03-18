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
package feast.storage.api.retrieval;

import feast.serving.ServingAPIProto;
import java.util.List;

/** Interface for implementing user defined retrieval functionality from Batch/historical stores. */
public interface BatchRetriever {

  /**
   * Get all features corresponding to the provided batch features request.
   *
   * @param request {@link ServingAPIProto.GetBatchFeaturesRequest} containing requested features
   *     and file containing entity columns.
   * @param featureSetRequests List of {@link FeatureSetRequest} to feature references in the
   *     request tied to that feature set.
   * @return {@link ServingAPIProto.Job} if successful, contains the location of the results, else
   *     contains the error to be returned to the user.
   */
  ServingAPIProto.Job getBatchFeatures(
      ServingAPIProto.GetBatchFeaturesRequest request, List<FeatureSetRequest> featureSetRequests);
}
