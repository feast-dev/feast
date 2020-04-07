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

import feast.serving.ServingAPIProto.DatasetSource;
import java.util.List;

/**
 * A historical retriever is a feature retriever that retrieves feature data corresponding to
 * provided entities over a given period of time.
 */
public interface HistoricalRetriever {

  /**
   * Get temporary staging location if applicable. If not applicable to this store, returns an empty
   * string.
   *
   * @return staging location uri
   */
  String getStagingLocation();

  /**
   * Get all features corresponding to the provided batch features request.
   *
   * @param retrievalId String that uniquely identifies this retrieval request.
   * @param datasetSource {@link DatasetSource} containing source to load the dataset containing
   *     entity columns.
   * @param featureSetRequests List of {@link FeatureSetRequest} to feature references in the
   *     request tied to that feature set.
   * @return {@link HistoricalRetrievalResult} if successful, contains the location of the results,
   *     else contains the error to be returned to the user.
   */
  HistoricalRetrievalResult getHistoricalFeatures(
      String retrievalId, DatasetSource datasetSource, List<FeatureSetRequest> featureSetRequests);
}
