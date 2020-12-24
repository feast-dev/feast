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
package feast.ingestion.metrics

import org.apache.spark.SparkEnv
import org.apache.spark.metrics.source.IngestionPipelineMetricSource
import org.apache.spark.sql.Row

class IngestionPipelineMetrics extends Serializable {

  def incrementDeadLetters(rowIterator: Iterator[Row]): Iterator[Row] = {
    val materialized = rowIterator.toArray
    if (metricSource.nonEmpty)
      metricSource.get.METRIC_DEADLETTER_ROWS_INSERTED.inc(materialized.length)

    materialized.toIterator
  }

  def incrementRead(rowIterator: Iterator[Row]): Iterator[Row] = {
    val materialized = rowIterator.toArray
    if (metricSource.nonEmpty)
      metricSource.get.METRIC_ROWS_READ_FROM_SOURCE.inc(materialized.length)

    materialized.toIterator
  }

  private lazy val metricSource: Option[IngestionPipelineMetricSource] = {
    val metricsSystem = SparkEnv.get.metricsSystem
    IngestionPipelineMetricsLock.synchronized {
      if (metricsSystem.getSourcesByName(IngestionPipelineMetricSource.sourceName).isEmpty) {
        metricsSystem.registerSource(new IngestionPipelineMetricSource)
      }
    }

    metricsSystem.getSourcesByName(IngestionPipelineMetricSource.sourceName) match {
      case Seq(head) => Some(head.asInstanceOf[IngestionPipelineMetricSource])
      case _         => None
    }
  }
}

private object IngestionPipelineMetricsLock
