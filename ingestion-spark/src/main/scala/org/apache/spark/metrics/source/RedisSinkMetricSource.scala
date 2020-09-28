package org.apache.spark.metrics.source

import com.codahale.metrics.MetricRegistry

class RedisSinkMetricSource extends Source {
  override val sourceName: String = "redis_sink"

  override val metricRegistry: MetricRegistry = new MetricRegistry

  val METRIC_TOTAL_ROWS_INSERTED = metricRegistry.counter(MetricRegistry.name("feast_ingestion_feature_row_ingested_count"))

  val METRIC_ROWS_LAG = metricRegistry.histogram(MetricRegistry.name("feast_ingestion_feature_row_lag_ms"))
}
