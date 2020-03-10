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

import feast.types.FeatureRowProto;

/** Response from an online store. */
public interface OnlineRetrieverResponse {

  /**
   * Checks whether the response is empty, i.e. feature does not exist in the store
   *
   * @return boolean
   */
  boolean isEmpty();

  /**
   * Get the featureset associated with this response.
   *
   * @return String featureset reference in format featureSet:version
   */
  String getFeatureSet();

  /**
   * Parse response to FeatureRow
   *
   * @return {@link FeatureRowProto.FeatureRow}
   */
  FeatureRowProto.FeatureRow toFeatureRow();
}
