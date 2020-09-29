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
package feast.ingestion

import feast.proto.types.ValueProto.ValueType
import org.joda.time.DateTime

object IngestionJob {
  import Modes._
  implicit val modesRead: scopt.Read[Modes.Value] = scopt.Read.reads(Modes withName _.capitalize)

  val parser = new scopt.OptionParser[IngestionJobConfig]("IngestionJon") {
    head("feast.ingestion.IngestionJob", "0.8")

    opt[Modes]("mode")
      .action((x, c) => c.copy(mode = x))
      .required()
      .text("Mode to operate ingestion job (offline or online)")

    opt[String](name = "feature-table-spec")
      .action((x, c) =>
        c.copy(featureTable =
          FeatureTable(
            name = "jaeger-car",
            project = "default",
            entities = Seq(Field(name = "customer", `type` = ValueType.Enum.STRING)),
            features = Seq(
              Field("car_customer_id_avg_customer_distance_cancelled", ValueType.Enum.FLOAT),
              Field("car_customer_id_num_completed", ValueType.Enum.FLOAT),
              Field("car_customer_id_origin_completed_1", ValueType.Enum.INT32)
            ),
            offline_source = Some(
              BQSource(
                "gods-staging",
                "feast",
                "default_jaeger_car_customer",
                Map.empty,
                "event_timestamp"
              )
            )
          )
        )
      )
      .required()
      .text("JSON-encoded FeatureTableSpec object")

    opt[String](name = "start")
      .action((x, c) => c.copy(startTime = DateTime.parse(x)))
      .text("Start timestamp for offline ingestion")

    opt[String](name = "end")
      .action((x, c) => c.copy(endTime = DateTime.parse(x)))
      .text("End timestamp for offline ingestion")
  }

  def main(args: Array[String]): Unit = {
    parser.parse(args, IngestionJobConfig()) match {
      case Some(config) =>
        config.mode match {
          case Modes.Offline =>
            val sparkSession = OfflinePipeline.createSparkSession(config)
            OfflinePipeline.createPipeline(sparkSession, config)
        }
      case None =>
        println("Parameters can't be parsed")
    }
  }

}
