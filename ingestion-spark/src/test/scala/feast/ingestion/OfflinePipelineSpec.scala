package feast.ingestion

import java.nio.file.Files

import com.dimafeng.testcontainers.{ForAllTestContainer, GenericContainer}
import com.google.protobuf.Timestamp
import feast.ingestion.utils.TypeConversion._
import feast.proto.types.ValueProto
import feast.proto.types.ValueProto.ValueType
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.joda.time.{DateTime, Seconds}
import org.scalacheck._
import org.scalatest._
import matchers.should.Matchers._

import org.scalatest.matchers.Matcher
import redis.clients.jedis.Jedis

import scala.jdk.CollectionConverters.mapAsScalaMapConverter
import scala.reflect.runtime.universe.TypeTag


case class Row(customer: String, feature1: Int, feature2: Float, eventTimestamp: java.sql.Timestamp)


class OfflinePipelineSpec extends UnitSpec with ForAllTestContainer with BeforeAndAfter {

  override val container = GenericContainer("redis:6.0.8", exposedPorts = Seq(6379))

  trait SparkContext {
    val sparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("Testing")
      .set("spark.redis.host", container.host)
      .set("spark.default.parallelism", "20")
      .set("spark.redis.port", container.mappedPort(6379).toString)

    val sparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
  }

  trait DataHelper {
    self: SparkContext =>

    def storeAsParquet[T <: Product : TypeTag](rows: Seq[T]): String = {
      import self.sparkSession.implicits._

      val tempPath = Files.createTempDirectory("test-dir").toFile.toString + "/rows"

      sparkSession.createDataset(rows)
        .withColumn("date", to_date($"eventTimestamp"))
        .write
        .partitionBy("date")
        .save(tempPath)

      tempPath
    }
  }

  trait Scope {
    val jedis = new Jedis("localhost", container.mappedPort(6379))
    jedis.flushAll()

    def rowGenerator(start: DateTime, end: DateTime, customerGen: Option[Gen[String]] = None) = for {
      customer <- customerGen.getOrElse(Gen.asciiPrintableStr)
      feature1 <- Gen.choose(0, 100)
      feature2 <- Gen.choose[Float](0, 1)
      eventTimestamp <- Gen.choose(0, Seconds.secondsBetween(start, end).getSeconds)
        .map(start.withMillisOfSecond(0).plusSeconds)
    }
      yield Row(customer, feature1, feature2, new java.sql.Timestamp(eventTimestamp.getMillis))

    def generateDistinctRows(gen: Gen[Row], N: Int) =
      Gen.listOfN(N, gen).sample.get.groupBy(_.customer).map(_._2.head).toSeq

    val config = IngestionJobConfig(
      featureTable = FeatureTable(
        name = "test-fs",
        entities = Seq(Field("customer", ValueType.Enum.STRING)),
        features = Seq(
          Field("feature1", ValueType.Enum.INT32),
          Field("feature2", ValueType.Enum.FLOAT),
        ),
      ),
      startTime = DateTime.parse("2020-08-01"),
      endTime = DateTime.parse("2020-09-01")
    )

    def beStoredRow(row: Row) = {
      val m: Matcher[Map[String, Any]] = contain.allElementsOf(Seq(
        "feature1" -> row.feature1,
        "feature2" -> row.feature2,
        "_ts:test-fs" -> row.eventTimestamp
      )).matcher

      m compose {
        (_: Map[Array[Byte], Array[Byte]])
          .map {
            case (k, v) => (
              new String(k),
              if (new String(k).startsWith("_ts"))
                Timestamp.parseFrom(v).asScala else
                ValueProto.Value.parseFrom(v).asScala
            )
          }
      }
    }
  }


  "Parquet source file" should "be ingested in redis" in new Scope with SparkContext with DataHelper {
    val gen = rowGenerator(DateTime.parse("2020-08-01"), DateTime.parse("2020-09-01"))
    val rows = generateDistinctRows(gen, 10000)
    val tempPath = storeAsParquet(rows)
    val configWithOfflineSource = config.copy(featureTable = config.featureTable.copy(offline_source =
      Some(GSSource(tempPath, Map.empty, "eventTimestamp"))))

    OfflinePipeline.createPipeline(sparkSession, configWithOfflineSource)

    rows.foreach(
      r => {
        val storedValues = jedis.hgetAll(s"customer:${r.customer}".getBytes).asScala.toMap
        storedValues should beStoredRow(r)
      }
    )
  }

  "Ingested rows" should "be compacted before storing" in new Scope with SparkContext with DataHelper {
    val entities = (0 to 10000).map(_.toString)

    val genLatest = rowGenerator(DateTime.parse("2020-08-15"), DateTime.parse("2020-09-01"), Some(Gen.oneOf(entities)))
    val latest = generateDistinctRows(genLatest, 10000)

    val genOld = rowGenerator(DateTime.parse("2020-08-01"), DateTime.parse("2020-08-14"), Some(Gen.oneOf(entities)))
    val old = generateDistinctRows(genOld, 10000)

    val tempPath = storeAsParquet(latest ++ old)
    val configWithOfflineSource = config.copy(featureTable = config.featureTable.copy(offline_source =
      Some(GSSource(tempPath, Map.empty, "eventTimestamp"))))

    OfflinePipeline.createPipeline(sparkSession, configWithOfflineSource)

    latest.foreach(
      r => {
        val storedValues = jedis.hgetAll(s"customer:${r.customer}".getBytes).asScala.toMap
        storedValues should beStoredRow(r)
      }
    )
  }

  "Old rows in ingestion" should "not overwrite more recent rows from storage" in new Scope with SparkContext with DataHelper {
    val entities = (0 to 10000).map(_.toString)

    val genLatest = rowGenerator(DateTime.parse("2020-08-15"), DateTime.parse("2020-09-01"), Some(Gen.oneOf(entities)))
    val latest = generateDistinctRows(genLatest, 10000)

    val tempPath1 = storeAsParquet(latest)
    val config1 = config.copy(featureTable = config.featureTable.copy(offline_source =
      Some(GSSource(tempPath1, Map.empty, "eventTimestamp"))))

    OfflinePipeline.createPipeline(sparkSession, config1)

    val genOld = rowGenerator(DateTime.parse("2020-08-01"), DateTime.parse("2020-08-14"), Some(Gen.oneOf(entities)))
    val old = generateDistinctRows(genOld, 10000)

    val tempPath2 = storeAsParquet(old)
    val config2 = config.copy(featureTable = config.featureTable.copy(offline_source =
      Some(GSSource(tempPath2, Map.empty, "eventTimestamp"))))

    OfflinePipeline.createPipeline(sparkSession, config2)

    latest.foreach(
      r => {
        val storedValues = jedis.hgetAll(s"customer:${r.customer}".getBytes).asScala.toMap
        storedValues should beStoredRow(r)
      }
    )
  }
}
