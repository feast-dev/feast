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
package feast.storage.api.statistics;

import com.google.protobuf.Timestamp;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import java.util.List;

public interface StatisticsRetriever {

  /**
   * Get feature statistics for a single feature set, for a single dataset id.
   *
   * @param featureSetSpec feature set spec of the requested feature set
   * @param features subset of features to retrieve.
   * @param ingestionId ingestion id to filter the data by
   * @return {@link FeatureStatistics} containing statistics for the requested features.
   */
  FeatureStatistics getFeatureStatistics(
      FeatureSetSpec featureSetSpec, List<String> features, String ingestionId);

  /**
   * Get feature statistics for a single feature set, for a single day.
   *
   * @param featureSetSpec feature set spec of the requested feature set
   * @param features subset of features to retrieve.
   * @param date date to filter the data by
   * @return {@link FeatureStatistics} containing statistics for the requested features.
   */
  FeatureStatistics getFeatureStatistics(
      FeatureSetSpec featureSetSpec, List<String> features, Timestamp date);
}
