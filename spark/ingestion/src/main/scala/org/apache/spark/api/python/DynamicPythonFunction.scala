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
package org.apache.spark.api.python

import java.io.File
import java.util.{ArrayList => JArrayList, HashMap => JHashMap}

import org.apache.commons.io.IOUtils
import org.apache.spark.SparkEnv
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.config.{PYSPARK_DRIVER_PYTHON, PYSPARK_PYTHON}

import collection.JavaConverters._

object DynamicPythonFunction {
  private val conf = SparkEnv.get.conf

  val pythonExec = conf
    .get(PYSPARK_DRIVER_PYTHON)
    .orElse(conf.get(PYSPARK_PYTHON))
    .orElse(sys.env.get("PYSPARK_DRIVER_PYTHON"))
    .orElse(sys.env.get("PYSPARK_PYTHON"))
    .getOrElse("python3")

  private def runCommand(cmd: List[String]): String = {
    val pb = new ProcessBuilder(cmd.asJava)
    val p  = pb.start()
    p.waitFor()
    IOUtils.toByteArray(p.getInputStream).map(_.toChar).mkString.trim
  }

  def pythonVersion: String = {
    runCommand(
      List(
        pythonExec,
        "-c",
        """import sys; print("{0.major}.{0.minor}".format(sys.version_info))"""
      )
    )
  }

  def pythonPlatform: String = {
    runCommand(
      List(pythonExec, "-c", "import platform; print(platform.system().lower())")
    )
  }

  def sparkHome: String = {
    runCommand(
      List(pythonExec, "-c", "import os; import pyspark; print(os.path.dirname(pyspark.__file__))")
    )
  }

  def create(
      pickledCode: Array[Byte],
      env: Map[String, String] = Map.empty,
      includePath: String = "libs/"
  ): PythonFunction = {
    val envVars    = new JHashMap[String, String](env.asJava)
    val broadcasts = new JArrayList[Broadcast[PythonBroadcast]]()

    if (!sys.env.contains("SPARK_HOME")) {
      // in tests there's no SPARK_HOME
      val libraries = List(
        Seq(sparkHome, "python", "lib", "pyspark.zip").mkString(File.separator),
        Seq(sparkHome, "python", "lib", "py4j-0.10.9-src.zip").mkString(File.separator)
      )
      envVars.put("PYTHONPATH", libraries.mkString(File.pathSeparator))
    }

    PythonFunction(
      pickledCode,
      envVars,
      List(includePath).asJava,
      pythonExec,
      pythonVersion,
      broadcasts,
      null
    )
  }

  def libsPathWithPlatform(libsPath: String): String =
    libsPath.replace("%(platform)s", s"py$pythonVersion-$pythonPlatform")
}
