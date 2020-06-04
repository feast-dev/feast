package feast.spark.ingestion

import java.util.UUID

import org.apache.spark.sql.SparkSession

/**
 * Consumes messages from one or more topics in Kafka and outputs them to the console.
 * Usage: SparkIngestion <bootstrap-servers> <topics>
 *     [<checkpoint-location>]
 *   <bootstrap-servers> The Kafka "bootstrap.servers" configuration. A
 *   comma-separated list of host:port.
 *   <topics> Comma-separated list of topics.
 *   <checkpoint-location> Directory in which to create checkpoints. If not
 *   provided, defaults to a randomized directory in /tmp.
 *
 * Example:
 *    `$SPARK_HOME/bin/spark-submit
 *      --master local
 *      --jars $HOME/.m2/repository/org/apache/spark/spark-sql-kafka-0-10_2.11/2.4.5/spark-sql-kafka-0-10_2.11-2.4.5.jar,$HOME/.m2/repository/org/apache/kafka/kafka-clients/2.3.0/kafka-clients-2.3.0.jar
 *      --class feast.spark.ingestion.SparkIngestion
 *      target/spark-ingestion-job-*.jar
 *      host1:port1,host2:port2 \
 *      topic1,topic2`
 */
object SparkIngestion {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: SparkIngestion <bootstrap-servers> " +
        "<topics> [<checkpoint-location>]")
      System.exit(1)
    }

    val Array(bootstrapServers, topics, _*) = args
    val checkpointLocation =
      if (args.length > 2) args(2) else "/tmp/temporary-" + UUID.randomUUID.toString

    // Create session with getOrCreate and do not call SparkContext.stop() at the end.
    // See https://docs.databricks.com/jobs.html#jar-job-tips
    val spark = SparkSession
      .builder
      .appName("SparkIngestion")
      .getOrCreate()

    import spark.implicits._

    // Create DataSet representing the stream of input lines from kafka
    val input = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topics)
      .load()

    // Start running the query that prints the data to the console
    val query = input.writeStream
      .format("console")
      .option("checkpointLocation", checkpointLocation)
      .start()

    query.awaitTermination()
  }
}
