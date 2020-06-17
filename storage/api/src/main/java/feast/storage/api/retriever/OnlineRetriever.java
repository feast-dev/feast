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
import java.util.Optional;

/** An online retriever is a feature retriever that retrieves the latest feature data. */
public interface OnlineRetriever {

  /**
   * Get online features for the given entity rows using data retrieved from the feature/featureset
   * specified in feature set request.
   *
   * <p>Each {@link FeatureRow} optional in the returned list then corresponds to an {@link
   * EntityRow} provided by the user. If feature for a given entity row is not found, will return an
   * empty optional instead. The no. of {@link FeatureRow} returned should match the no. of given
   * {@link EntityRow}s
   *
   * @param entityRows list of entity rows to request features for.
   * @param featureSetRequest specifies the features/feature set to retrieve data from
   * @return list of {@link FeatureRow}s corresponding to data retrieved for each entity row from
   *     feature/featureset specified in featureset request.
   */
  List<Optional<FeatureRow>> getOnlineFeatures(
      List<EntityRow> entityRows, FeatureSetRequest featureSetRequest);
}
