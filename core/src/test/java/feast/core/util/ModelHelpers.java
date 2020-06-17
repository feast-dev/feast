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
package feast.core.util;

import feast.core.model.FeatureSet;
import feast.core.model.FeatureSetJobStatus;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ModelHelpers {
  public static Set<FeatureSetJobStatus> makeFeatureSetJobStatus(FeatureSet... featureSets) {
    return Stream.of(featureSets)
        .map(
            fs -> {
              FeatureSetJobStatus s = new FeatureSetJobStatus();
              s.setFeatureSet(fs);
              return s;
            })
        .collect(Collectors.toSet());
  }

  public static Set<FeatureSetJobStatus> makeFeatureSetJobStatus(List<FeatureSet> featureSets) {
    return makeFeatureSetJobStatus(featureSets.toArray(FeatureSet[]::new));
  }
}
