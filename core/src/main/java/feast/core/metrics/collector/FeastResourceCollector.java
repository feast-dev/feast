/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
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
package feast.core.metrics.collector;

import feast.core.dao.FeatureTableRepository;
import feast.core.dao.StoreRepository;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import java.util.ArrayList;
import java.util.List;

/**
 * FeastResourceCollector exports metrics about Feast Resources.
 *
 * <p>For example: total number of registered feature tables and stores.
 */
public class FeastResourceCollector extends Collector {

  private final FeatureTableRepository featureTableRepository;
  private final StoreRepository storeRepository;

  public FeastResourceCollector(
      FeatureTableRepository featureTableRepository, StoreRepository storeRepository) {
    this.featureTableRepository = featureTableRepository;
    this.storeRepository = storeRepository;
  }

  @Override
  public List<MetricFamilySamples> collect() {
    List<MetricFamilySamples> samples = new ArrayList<>();
    samples.add(
        new GaugeMetricFamily(
            "feast_core_feature_set_total",
            "Total number of registered feature tables",
            featureTableRepository.count()));
    samples.add(
        new GaugeMetricFamily(
            "feast_core_store_total",
            "Total number of registered stores",
            storeRepository.count()));
    return samples;
  }
}
