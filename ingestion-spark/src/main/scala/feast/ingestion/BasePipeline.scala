package feast.ingestion

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait BasePipeline {
  def createSparkSession(jobConfig: IngestionJobConfig): SparkSession = {
    // workaround for issue with arrow & netty
    // see https://github.com/apache/arrow/tree/master/java#java-properties
    System.setProperty("io.netty.tryReflectionSetAccessible", "true")

    val conf = new SparkConf()
    conf
      .setAppName(s"${jobConfig.mode} IngestionJob for ${jobConfig.featureTable.name}")
      .setMaster("local")

    jobConfig.metrics match {
      case Some(c: StatsDConfig) =>
        conf
          .set("spark.metrics.conf.*.source.redis.class", "org.apache.spark.metrics.source.RedisSinkMetricSource")
          .set("spark.metrics.conf.*.sink.statsd.class", "org.apache.spark.metrics.sink.StatsdSink")
          .set("spark.metrics.conf.*.sink.statsd.host", c.host)
          .set("spark.metrics.conf.*.sink.statsd.port", c.port.toString)
          .set("spark.metrics.conf.*.sink.statsd.period", "1")
          .set("spark.metrics.conf.*.sink.statsd.unit", "seconds")
    }

    SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
  }


  def createPipeline(sparkSession: SparkSession, config: IngestionJobConfig): Unit
}
