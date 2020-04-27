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
package feast.storage.api.retriever;

import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest.EntityRow;
import feast.types.FeatureRowProto.FeatureRow;
import java.util.List;

/**
 * An online retriever is a feature retriever that retrieves the latest feature data corresponding
 * to provided entities.
 */
public interface OnlineRetriever {

  /**
   * Get all values corresponding to the request.
   *
   * @param entityRows list of entity rows in the feature request
   * @param featureSetRequests List of {@link FeatureSetRequest} to feature references in the
   *     request tied to that feature set.
   * @return list of lists of {@link FeatureRow}s corresponding to each feature set request and
   *     entity row.
   */
  List<List<FeatureRow>> getOnlineFeatures(
      List<EntityRow> entityRows, List<FeatureSetRequest> featureSetRequests);
}
