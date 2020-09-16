package feast.ingestion

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


class StreamIngestionApp {
  def createSparkSession[T](contextName: String,
                            sqlJoinPartitions: Int = 10
                           ): SparkSession = {
    val conf = new SparkConf()
    conf
      .setAppName(contextName)
      .set("spark.sql.shuffle.partitions", sqlJoinPartitions.toString)

    val sparkSession = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
      .config(conf)
      .getOrCreate()

    sparkSession
  }

  case class Config(
                   sourceBootstrapServers: String = "",
                   sourceTopic: String = "",
                   specsBootstrapServers: String = "",
                   specsTopic: String = ""
                   )

  val parser = new scopt.OptionParser[Config]("scopt") {
    head("scopt", "3.x")

    opt[String]("source-broker")
      .action((x, c) => c.copy(sourceBootstrapServers = x))

    opt[String]("source-topic")
      .action((x, c) => c.copy(sourceTopic = x))

    opt[String]("specs-broker")
      .action((x, c) => c.copy(specsBootstrapServers = x))

    opt[String]("specs-broker")
      .action((x, c) => c.copy(specsTopic = x))
  }

  def start(args: Array[String]) {
    val config = parser.parse(args, Config()).get
    val spark = createSparkSession("TestApp")

    import spark.implicits._

    val rows = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.sourceBootstrapServers)
      .option("subscribe", config.sourceTopic)
      .load()
      .selectExpr("CAST(key as STRING)", "value")
      .as[(String, Array[Byte])]

    val specs = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.specsBootstrapServers)
      .option("subscribe", config.specsTopic)
      .load()
      .selectExpr("CASE(key as STRING)", "value")
      .as[(String, Array[Byte])]

    rows.printSchema()
    specs.printSchema()
  }
}

object StreamIngestionApp extends App {
  val app = new StreamIngestionApp
  app.start(args)
}
