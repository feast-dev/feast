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

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import org.tensorflow.metadata.v0.FeatureNameStatistics;

/** Feature statistics over a bounded set of data. */
@AutoValue
public abstract class FeatureStatistics {

  public abstract long getNumExamples();

  public abstract ImmutableList<FeatureNameStatistics> getFeatureNameStatistics();

  public static Builder newBuilder() {
    return new AutoValue_FeatureStatistics.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setNumExamples(long numExamples);

    protected abstract ImmutableList.Builder<FeatureNameStatistics> featureNameStatisticsBuilder();

    public Builder addFeatureNameStatistics(FeatureNameStatistics featureNameStatistics) {
      featureNameStatisticsBuilder().add(featureNameStatistics);
      return this;
    }

    public abstract FeatureStatistics build();
  }
}
