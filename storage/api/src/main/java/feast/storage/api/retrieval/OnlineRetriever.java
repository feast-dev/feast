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

import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest;
import feast.types.FeatureRowProto;
import java.io.Serializable;
import java.util.List;

/** Interface for implementing user defined retrieval functionality from Online stores. */
public interface OnlineRetriever<T> extends Serializable {

  /**
   * Get all values corresponding to the request.
   *
   * @param request Online features request containing the list of features and list of entity rows
   * @return list of {@link T}
   */
  List<T> getOnlineFeatures(GetOnlineFeaturesRequest request);

  /**
   * Checks whether the response is empty, i.e. feature does not exist in the store
   *
   * @param response {@link T}
   * @return boolean
   */
  boolean isEmpty(T response);

  /**
   * Parse response from data store to FeatureRow
   *
   * @param response {@link T}
   * @return {@link FeatureRowProto.FeatureRow}
   */
  FeatureRowProto.FeatureRow parseResponse(T response);
}
