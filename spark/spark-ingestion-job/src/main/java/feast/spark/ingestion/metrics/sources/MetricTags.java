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
package feast.spark.ingestion.metrics.sources;

import com.google.auto.value.AutoValue;
import feast.common.models.FeatureSetReference;
import javax.annotation.Nullable;

@AutoValue
public abstract class MetricTags {

  @Nullable
  public abstract String getMetricsNamespace();

  @Nullable
  public abstract FeatureSetReference getFeatureSetRef();

  @Nullable
  public abstract String getFeatureName();

  @Nullable
  public abstract String getStoreName();

  public static MetricTags.Builder newBuilder() {
    return new AutoValue_MetricTags.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract MetricTags.Builder setMetricsNamespace(String metricsNamespace);

    public abstract MetricTags.Builder setFeatureSetRef(FeatureSetReference featureSetReference);

    public abstract MetricTags.Builder setFeatureName(String featureName);

    public abstract MetricTags.Builder setStoreName(String storeName);

    public abstract MetricTags build();
  }
}
