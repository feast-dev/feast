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

import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import io.prometheus.client.SummaryMetricFamily;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * JVMResourceCollector exports metrics about Java virtual machine memory and garbage collection.
 */
public class JVMResourceCollector extends Collector {

  private final List<GarbageCollectorMXBean> garbageCollectors;
  private final Runtime runtime;

  public JVMResourceCollector() {
    garbageCollectors = ManagementFactory.getGarbageCollectorMXBeans();
    runtime = Runtime.getRuntime();
  }

  @Override
  public List<MetricFamilySamples> collect() {
    List<MetricFamilySamples> samples = new ArrayList<>();

    samples.add(
        new GaugeMetricFamily(
            "feast_core_max_memory_bytes",
            "Max amount of memory the Java virtual machine will attempt to use",
            runtime.maxMemory()));
    samples.add(
        new GaugeMetricFamily(
            "feast_core_total_memory_bytes",
            "Total amount of memory in the Java virtual machine",
            runtime.totalMemory()));
    samples.add(
        new GaugeMetricFamily(
            "feast_core_free_memory_bytes",
            "Total amount of free memory in the Java virtual machine",
            runtime.freeMemory()));

    SummaryMetricFamily gcMetricFamily =
        new SummaryMetricFamily(
            "feast_core_gc_collection_seconds",
            "Time spent in a given JVM garbage collector in seconds",
            Collections.singletonList("gc"));
    for (final GarbageCollectorMXBean gc : garbageCollectors) {
      gcMetricFamily.addMetric(
          Collections.singletonList(gc.getName()),
          gc.getCollectionCount(),
          gc.getCollectionTime() / MILLISECONDS_PER_SECOND);
    }
    samples.add(gcMetricFamily);

    return samples;
  }
}
