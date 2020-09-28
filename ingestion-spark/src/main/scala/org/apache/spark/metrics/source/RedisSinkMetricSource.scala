package org.apache.spark.metrics.source

import com.codahale.metrics.MetricRegistry

class RedisSinkMetricSource extends Source {
  override val sourceName: String = "redis_sink"

  override val metricRegistry: MetricRegistry = new MetricRegistry

  val METRIC_TOTAL_ROWS_INSERTED = metricRegistry.counter(MetricRegistry.name("rowsInserted"))
}
