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
package feast.spark.ingestion.metrics;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.DoubleSummaryStatistics;
import java.util.List;

@AutoValue
public abstract class FeatureValueSummaryStatistics implements Serializable {

  public abstract DoubleSummaryStatistics getFeatureNameToStats();

  public abstract List<Double> getFeatureNameToValues();

  public static FeatureValueSummaryStatistics.Builder newBuilder() {
    return new AutoValue_FeatureValueSummaryStatistics.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract FeatureValueSummaryStatistics.Builder setFeatureNameToStats(
        DoubleSummaryStatistics featureNameToStats);

    public abstract FeatureValueSummaryStatistics.Builder setFeatureNameToValues(
        List<Double> featureNameToValues);

    public abstract FeatureValueSummaryStatistics build();
  }
}
