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
import feast.spark.ingestion.metrics.gauges.SimpleGauge;
import java.io.Serializable;

@AutoValue
public abstract class FeatureRowMetrics implements Serializable {

  public abstract SimpleGauge<Double> getMinGauge();

  public abstract SimpleGauge<Double> getMaxGauge();

  public abstract SimpleGauge<Double> getMeanGauge();

  public static FeatureRowMetrics.Builder newBuilder() {
    return new AutoValue_FeatureRowMetrics.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract FeatureRowMetrics.Builder setMinGauge(SimpleGauge<Double> gaugeMin);

    public abstract FeatureRowMetrics.Builder setMaxGauge(SimpleGauge<Double> gaugeMax);

    public abstract FeatureRowMetrics.Builder setMeanGauge(SimpleGauge<Double> gaugeMean);

    public abstract FeatureRowMetrics build();
  }
}
