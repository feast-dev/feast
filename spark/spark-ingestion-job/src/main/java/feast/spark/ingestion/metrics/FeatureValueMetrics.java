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
public abstract class FeatureValueMetrics implements Serializable {

  public abstract SimpleGauge<Double> getMinGauge();

  public abstract SimpleGauge<Double> getMaxGauge();

  public abstract SimpleGauge<Double> getMeanGauge();

  public abstract SimpleGauge<Double> getP25Gauge();

  public abstract SimpleGauge<Double> getP50Gauge();

  public abstract SimpleGauge<Double> getP90Gauge();

  public abstract SimpleGauge<Double> getP95Gauge();

  public abstract SimpleGauge<Double> getP99Gauge();

  public static FeatureValueMetrics.Builder newBuilder() {
    return new AutoValue_FeatureValueMetrics.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract FeatureValueMetrics.Builder setMinGauge(SimpleGauge<Double> gaugeMin);

    public abstract FeatureValueMetrics.Builder setMaxGauge(SimpleGauge<Double> gaugeMax);

    public abstract FeatureValueMetrics.Builder setMeanGauge(SimpleGauge<Double> gaugeMean);

    public abstract FeatureValueMetrics.Builder setP25Gauge(SimpleGauge<Double> gaugeP25);

    public abstract FeatureValueMetrics.Builder setP50Gauge(SimpleGauge<Double> gaugeP50);

    public abstract FeatureValueMetrics.Builder setP90Gauge(SimpleGauge<Double> gaugeP90);

    public abstract FeatureValueMetrics.Builder setP95Gauge(SimpleGauge<Double> gaugeP95);

    public abstract FeatureValueMetrics.Builder setP99Gauge(SimpleGauge<Double> gaugeP99);

    public abstract FeatureValueMetrics build();
  }
}
