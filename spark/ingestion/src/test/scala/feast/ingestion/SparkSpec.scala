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

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter

class SparkSpec extends UnitSpec with BeforeAndAfter {
  var sparkSession: SparkSession                         = null
  def withSparkConfOverrides(conf: SparkConf): SparkConf = conf

  before {
    val sparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("Testing")
      .set("spark.default.parallelism", "8")

    sparkSession = SparkSession
      .builder()
      .config(withSparkConfOverrides(sparkConf))
      .getOrCreate()
  }

  after {
    sparkSession.stop()
  }
}
