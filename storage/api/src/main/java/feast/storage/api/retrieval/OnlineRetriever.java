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

import feast.core.FeatureSetProto;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest;
import java.util.List;

/** Interface for implementing user defined retrieval functionality from Online stores. */
public interface OnlineRetriever {

  /**
   * Get all values corresponding to the request.
   *
   * @param request Online features request containing the list of features and list of entity rows
   * @param featureSetSpecs List of {@link feast.core.FeatureSetProto.FeatureSetSpec} passed to the
   *     retriever from the serving service. The specs will only contain the features requested by
   *     the user.
   * @return list of {@link OnlineRetrieverResponse}
   */
  List<OnlineRetrieverResponse> getOnlineFeatures(
      GetOnlineFeaturesRequest request, List<FeatureSetProto.FeatureSetSpec> featureSetSpecs);
}
