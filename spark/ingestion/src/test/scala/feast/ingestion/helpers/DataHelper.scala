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
package feast.ingestion.helpers

import java.nio.file.{Files, Paths}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.to_date
import org.scalacheck.Gen
import scala.reflect.runtime.universe.TypeTag

object DataHelper {
  def generateRows[A](gen: Gen[A], N: Int): Seq[A] =
    Gen.listOfN(N, gen).sample.get

  def generateDistinctRows[A](gen: Gen[A], N: Int, entityFun: A => String): Seq[A] =
    generateRows(gen, N).groupBy(entityFun).map(_._2.head).toSeq

  def generateTempPath(last: String): String =
    Paths.get(Files.createTempDirectory("test-dir").toString, last).toString

  def storeAsParquet[T <: Product: TypeTag](sparkSession: SparkSession, rows: Seq[T]): String = {
    import sparkSession.implicits._

    val tempPath = generateTempPath("rows")

    sparkSession
      .createDataset(rows)
      .withColumn("date", to_date($"eventTimestamp"))
      .write
      .partitionBy("date")
      .save(tempPath)

    tempPath
  }
}
