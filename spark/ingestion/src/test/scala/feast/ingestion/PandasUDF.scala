package feast.ingestion


import org.apache.spark.api.python.DynamicPythonFunction
import org.apache.spark.sql.{Dataset, Encoder, Row, SQLContext}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, MapInPandas}
import org.apache.spark.sql.execution.python.UserDefinedPythonFunction
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.types.{BooleanType, StructType}
import org.apache.spark.sql.functions.{struct, window}
import org.apache.spark.sql.streaming.Trigger

case class NewTestRow(num: Int, sqr: Long)

class PandasUDF extends SparkSpec {
  System.setProperty("io.netty.tryReflectionSetAccessible", "true")

  trait Scope {
    implicit def testRowEncoder: Encoder[NewTestRow] = ExpressionEncoder()
    implicit def sqlContext: SQLContext = sparkSession.sqlContext

    val SQL_SCALAR_PANDAS_UDF = 200
    val SQL_MAP_PANDAS_UDF = 205

    sparkSession.sparkContext.addFile(getClass.getResource("/python/libs.tar.gz").getPath)

    val pythonFun = DynamicPythonFunction.create(
      getClass.getResourceAsStream("/python/udf.pickle").readAllBytes()
    )

    val udf: UserDefinedPythonFunction = UserDefinedPythonFunction(
      "validate",
      pythonFun,
      BooleanType,
      SQL_SCALAR_PANDAS_UDF,
      udfDeterministic = true
    )
  }

  "Python UDF" should "work" in new Scope {
    val inputData = MemoryStream[NewTestRow]
    val df = inputData.toDF()

    val cols = df.columns.map(df(_))
    val streamingQuery = df
      .withColumn("valid", udf(struct(cols:_*)))
      .writeStream
      .format("memory")
      .queryName("sink")
      .start()

    val data = (1 to 100).map(i => NewTestRow(i, i * i))
    inputData.addData(data)
    streamingQuery.processAllAvailable()

    sparkSession.sql("select * from sink").show()
  }
}
