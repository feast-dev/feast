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

import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequest.EntityRow;
import feast.proto.types.FeatureRowProto.FeatureRow;
import java.util.List;

/** An online retriever is a feature retriever that retrieves the latest feature data. */
public interface OnlineRetriever {

  /**
   * Get online features for the given entity rows using data retrieved from the feature/featureset
   * specified in feature set requests.
   *
   * <p>This method returns a list of {@link FeatureRow}s corresponding to each feature set spec.
   * Each feature row in the list then corresponds to an {@link EntityRow} provided by the user. If
   * feature for a given entity row is not found, will return null in place of the {@link FeatureRow}.
   *
   * @param entityRows list of entity rows to request.
   * @param featureSetRequests List of {@link FeatureSetRequest} specifying the features/feature set
   *     to retrieve data from.
   * @return list of lists of {@link FeatureRow}s corresponding to data retrieved for each feature
   *     set request and entity row.
   */
  List<List<FeatureRow>> getOnlineFeatures(
      List<EntityRow> entityRows, List<FeatureSetRequest> featureSetRequests);
}
