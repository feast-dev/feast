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

import org.joda.time.DateTime

import org.json4s._
import org.json4s.jackson.JsonMethods.{parse => parseJSON}
import org.json4s.ext.JavaEnumNameSerializer

object IngestionJob {
  import Modes._
  implicit val modesRead: scopt.Read[Modes.Value] = scopt.Read.reads(Modes withName _.capitalize)
  implicit val formats: Formats = DefaultFormats +
    new JavaEnumNameSerializer[feast.proto.types.ValueProto.ValueType.Enum]() +
    ShortTypeHints(List(classOf[ProtoFormat], classOf[AvroFormat]))

  val parser = new scopt.OptionParser[IngestionJobConfig]("IngestionJon") {
    // ToDo: read version from Manifest
    head("feast.ingestion.IngestionJob", "0.8-SNAPSHOT")

    opt[Modes]("mode")
      .action((x, c) => c.copy(mode = x))
      .required()
      .text("Mode to operate ingestion job (offline or online)")

    opt[String](name = "source")
      .action((x, c) =>
        parseJSON(x).camelizeKeys.extract[Sources] match {
          case Sources(file: Some[FileSource], _, _)   => c.copy(source = file.get)
          case Sources(_, bq: Some[BQSource], _)       => c.copy(source = bq.get)
          case Sources(_, _, kafka: Some[KafkaSource]) => c.copy(source = kafka.get)
        }
      )
      .required()
      .text("JSON-encoded source object (e.g. {\"kafka\":{\"bootstrapServers\":...}}")

    opt[String](name = "feature-table")
      .action((x, c) => c.copy(featureTable = parseJSON(x).camelizeKeys.extract[FeatureTable]))
      .required()
      .text("JSON-encoded FeatureTableSpec object")

    opt[String](name = "start")
      .action((x, c) => c.copy(startTime = DateTime.parse(x)))
      .text("Start timestamp for offline ingestion")

    opt[String](name = "end")
      .action((x, c) => c.copy(endTime = DateTime.parse(x)))
      .text("End timestamp for offline ingestion")

    opt[String](name = "redis")
      .action((x, c) => c.copy(store = parseJSON(x).extract[RedisConfig]))

    opt[String](name = "statsd")
      .action((x, c) => c.copy(metrics = Some(parseJSON(x).extract[StatsDConfig])))

    opt[String](name = "deadletter-path")
      .action((x, c) => c.copy(deadLetterPath = Some(x)))

  }

  def main(args: Array[String]): Unit = {
    parser.parse(args, IngestionJobConfig()) match {
      case Some(config) =>
        println(s"Starting with config $config")
        config.mode match {
          case Modes.Offline =>
            val sparkSession = BatchPipeline.createSparkSession(config)
            BatchPipeline.createPipeline(sparkSession, config)
          case Modes.Online =>
            val sparkSession = BatchPipeline.createSparkSession(config)
            StreamingPipeline.createPipeline(sparkSession, config).get.awaitTermination
        }
      case None =>
        println("Parameters can't be parsed")
    }
  }

}
