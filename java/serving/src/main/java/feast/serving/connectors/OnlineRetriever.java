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
package feast.serving.connectors;

import feast.proto.serving.ServingAPIProto;
import feast.proto.types.ValueProto;
import java.util.List;
import java.util.Map;

public interface OnlineRetriever {
  /**
   * Get online features for the given entity rows using data retrieved from the Feature references
   * specified in FeatureTable request.
   *
   * <p>Each {@link Feature} optional in the returned list then corresponds to an {@link
   * ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow} provided by the user. If feature for a
   * given entity row is not found, will return an empty optional instead. The no. of {@link
   * Feature} returned should match the no. of given {@link
   * ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow}s
   *
   * @param entityRows list of entity rows to request features for.
   * @param featureReferences specifies the FeatureTable to retrieve data from
   * @param entityNames name of entities
   * @return list of {@link Feature}s corresponding to data retrieved for each entity row from
   *     FeatureTable specified in FeatureTable request.
   */
  List<List<Feature>> getOnlineFeatures(
      List<Map<String, ValueProto.Value>> entityRows,
      List<ServingAPIProto.FeatureReferenceV2> featureReferences,
      List<String> entityNames);
}
