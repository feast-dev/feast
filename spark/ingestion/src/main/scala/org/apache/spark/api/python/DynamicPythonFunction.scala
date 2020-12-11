package org.apache.spark.api.python

import java.io.File
import java.util.{ArrayList => JArrayList, HashMap => JHashMap}

import org.apache.spark.broadcast.Broadcast
import collection.JavaConverters._

object DynamicPythonFunction {
  val pythonExec = "python3"

  private def runCommand(cmd: List[String]): String = {
    val pb = new ProcessBuilder(cmd.asJava)
    val p = pb.start()
    p.waitFor()
    p.getInputStream.readAllBytes().map(_.toChar).mkString.trim
  }

  def pythonVersion: String = {
    runCommand(List(
      pythonExec,
      "-c",
      "import sys; print(\"{0.major}.{0.minor}\".format(sys.version_info))"))
  }

  def sparkHome: String = {
    runCommand(List(
      pythonExec,
      "-c",
      "import os; import pyspark; print(os.path.dirname(pyspark.__file__))"))
  }

  def create(pickledCode: Array[Byte], dependenciesPath: String = "libs/"): PythonFunction = {
    val envVars = new JHashMap[String, String]()
    val broadcasts = new JArrayList[Broadcast[PythonBroadcast]]()

    if (!sys.env.contains("SPARK_HOME")) {
      // in tests there's no SPARK_HOME
      val libraries = List(
        Seq(sparkHome, "python", "lib", "pyspark.zip").mkString(File.separator),
        Seq(sparkHome, "python", "lib", "py4j-0.10.9-src.zip").mkString(File.separator),
      )
      envVars.put("PYTHONPATH", libraries.mkString(File.pathSeparator))
    }

    PythonFunction(
      pickledCode,
      envVars,
      List(dependenciesPath).asJava,
      pythonExec,
      pythonVersion,
      broadcasts,
      null
    )
  }
}
