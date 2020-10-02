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
package org.apache.spark.metrics.source

import com.codahale.metrics.MetricRegistry
import org.apache.spark.{SparkConf, SparkEnv}

class RedisSinkMetricSource extends Source {
  override val sourceName: String = RedisSinkMetricSource.sourceName

  override val metricRegistry: MetricRegistry = new MetricRegistry

  private val sparkConfig = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf(true))

  private val metricLabels = sparkConfig.get("spark.metrics.conf.*.source.redis.labels")

  private def nameWithLabels(name: String) =
    if (metricLabels.isEmpty) {
      name
    } else {
      s"$name#$metricLabels"
    }

  val METRIC_TOTAL_ROWS_INSERTED =
    metricRegistry.counter(nameWithLabels("feast_ingestion_feature_row_ingested_count"))

  val METRIC_ROWS_LAG =
    metricRegistry.histogram(nameWithLabels("feast_ingestion_feature_row_lag_ms"))
}

object RedisSinkMetricSource {
  val sourceName = "redis_sink"
}
