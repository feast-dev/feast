package feast.ingestion

import java.nio.file.{Files, Paths}

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

import scala.jdk.CollectionConverters._
import scala.reflect.runtime.universe.TypeTag
import scala.util.hashing.MurmurHash3

case class Row(customer: String, feature1: Int, feature2: Float, eventTimestamp: java.sql.Timestamp)


class OfflinePipelineIT extends UnitSpec with ForAllTestContainer {

  override val container = GenericContainer("redis:6.0.8", exposedPorts = Seq(6379))

  trait SparkContext {
    val sparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("Testing")
      .set("spark.redis.host", container.host)
      .set("spark.default.parallelism", "8")
      .set("spark.redis.port", container.mappedPort(6379).toString)

    val sparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
  }

  trait DataHelper {
    self: SparkContext =>

    def generateTempPath(last: String) =
      Paths.get(Files.createTempDirectory("test-dir").toString, last).toString

    def storeAsParquet[T <: Product : TypeTag](rows: Seq[T]): String = {
      import self.sparkSession.implicits._

      val tempPath = generateTempPath("rows")

      sparkSession.createDataset(rows)
        .withColumn("date", to_date($"eventTimestamp"))
        .write
        .partitionBy("date")
        .save(tempPath)

      tempPath
    }
  }

  trait Scope extends SparkContext with DataHelper {
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
        project = "default",
        entities = Seq(Field("customer", ValueType.Enum.STRING)),
        features = Seq(
          Field("feature1", ValueType.Enum.INT32),
          Field("feature2", ValueType.Enum.FLOAT),
        ),
      ),
      startTime = DateTime.parse("2020-08-01"),
      endTime = DateTime.parse("2020-09-01")
    )

    def encodeEntityKey(row: Row, featureTable: FeatureTable) = {
      val entityPrefix = featureTable.entities.map(_.name).mkString("_")
      s"${featureTable.project}_${entityPrefix}:${row.customer}".getBytes
    }

    def encodeFeatureKey(featureTable: FeatureTable)(feature: String): String = {
      val fullReference = s"${featureTable.name}:$feature"
      MurmurHash3.stringHash(fullReference).toHexString
    }

    def beStoredRow(mappedRow: Map[String, Any]) = {
      val m: Matcher[Map[String, Any]] = contain.allElementsOf(mappedRow).matcher

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

  "Parquet source file" should "be ingested in redis" in new Scope {
    val gen = rowGenerator(DateTime.parse("2020-08-01"), DateTime.parse("2020-09-01"))
    val rows = generateDistinctRows(gen, 10000)
    val tempPath = storeAsParquet(rows)
    val configWithOfflineSource = config.copy(featureTable = config.featureTable.copy(offline_source =
      Some(GSSource(tempPath, Map.empty, "eventTimestamp"))))

    OfflinePipeline.createPipeline(sparkSession, configWithOfflineSource)

    val featureKeyEncoder: String => String = encodeFeatureKey(config.featureTable)

    rows.foreach(
      r => {
        val storedValues = jedis.hgetAll(encodeEntityKey(r, config.featureTable)).asScala.toMap
        storedValues should beStoredRow(Map(
          featureKeyEncoder("feature1") -> r.feature1,
          featureKeyEncoder("feature2") -> r.feature2,
          "_ts:test-fs" -> r.eventTimestamp
        ))
      }
    )
  }

  "Ingested rows" should "be compacted before storing by timestamp column" in new Scope {
    val entities = (0 to 10000).map(_.toString)

    val genLatest = rowGenerator(DateTime.parse("2020-08-15"), DateTime.parse("2020-09-01"), Some(Gen.oneOf(entities)))
    val latest = generateDistinctRows(genLatest, 10000)

    val genOld = rowGenerator(DateTime.parse("2020-08-01"), DateTime.parse("2020-08-14"), Some(Gen.oneOf(entities)))
    val old = generateDistinctRows(genOld, 10000)

    val tempPath = storeAsParquet(latest ++ old)
    val configWithOfflineSource = config.copy(featureTable = config.featureTable.copy(offline_source =
      Some(GSSource(tempPath, Map.empty, "eventTimestamp"))))

    OfflinePipeline.createPipeline(sparkSession, configWithOfflineSource)

    val featureKeyEncoder: String => String = encodeFeatureKey(config.featureTable)

    latest.foreach(
      r => {
        val storedValues = jedis.hgetAll(encodeEntityKey(r, config.featureTable)).asScala.toMap
        storedValues should beStoredRow(Map(
          featureKeyEncoder("feature1") -> r.feature1,
          featureKeyEncoder("feature2") -> r.feature2,
          "_ts:test-fs" -> r.eventTimestamp
        ))
      }
    )
  }

  "Old rows in ingestion" should "not overwrite more recent rows from storage" in new Scope {
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

    val featureKeyEncoder: String => String = encodeFeatureKey(config.featureTable)

    latest.foreach(
      r => {
        val storedValues = jedis.hgetAll(encodeEntityKey(r, config.featureTable)).asScala.toMap
        storedValues should beStoredRow(Map(
          featureKeyEncoder("feature1") -> r.feature1,
          featureKeyEncoder("feature2") -> r.feature2,
          "_ts:test-fs" -> r.eventTimestamp
        ))
      }
    )
  }

  "Invalid rows" should "not be ingested and stored to deadletter instead" in new Scope {
    val gen = rowGenerator(DateTime.parse("2020-08-01"), DateTime.parse("2020-09-01"))
    val rows = generateDistinctRows(gen, 100)

    val rowsWithNullEntity = rows.map(_.copy(customer = null))

    val tempPath = storeAsParquet(rowsWithNullEntity)
    val deadletterConfig = config.copy(
      featureTable = config.featureTable.copy(offline_source = Some(GSSource(tempPath, Map.empty, "eventTimestamp"))),
      deadLetterPath = Some(generateTempPath("deadletters"))
    )

    OfflinePipeline.createPipeline(sparkSession, deadletterConfig)

    jedis.keys("*").toArray should be (empty)

    sparkSession.read
      .parquet(deadletterConfig.deadLetterPath.get)
      .count() should be (rows.length)
  }

  "Columns from source" should "be mapped according to configuration" in new Scope {
    val gen = rowGenerator(DateTime.parse("2020-08-01"), DateTime.parse("2020-09-01"))
    val rows = generateDistinctRows(gen, 100)

    val tempPath = storeAsParquet(rows)

    val configWithMapping = config.copy(
      featureTable = config.featureTable.copy(
        entities = Seq(Field("entity", ValueType.Enum.STRING)),
        features = Seq(
          Field("new_feature1", ValueType.Enum.INT32),
          Field("new_feature2", ValueType.Enum.FLOAT)
        ),
        offline_source = Some(GSSource(
          tempPath,
          Map(
            "entity" -> "customer",
            "new_feature1" -> "feature1",
            "new_feature2" -> "feature2"
          ),
          "eventTimestamp")))
    )

    OfflinePipeline.createPipeline(sparkSession, configWithMapping)

    val featureKeyEncoder: String => String = encodeFeatureKey(config.featureTable)

    rows.foreach(
      r => {
        val storedValues = jedis.hgetAll(encodeEntityKey(r, configWithMapping.featureTable)).asScala.toMap
        storedValues should beStoredRow(Map(
          featureKeyEncoder("new_feature1") -> r.feature1,
          featureKeyEncoder("new_feature2") -> r.feature2,
          "_ts:test-fs" -> r.eventTimestamp
        ))
      }
    )
  }
}
