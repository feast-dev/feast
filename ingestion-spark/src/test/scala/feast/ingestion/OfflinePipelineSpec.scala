package feast.ingestion

import java.nio.file.Files

import com.dimafeng.testcontainers.{ForAllTestContainer, GenericContainer}
import com.google.protobuf.Timestamp
import feast.ingestion.utils.TypeConversion.protoValueAsScala
import feast.proto.types.ValueProto
import feast.proto.types.ValueProto.ValueType
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import org.scalacheck._
import org.scalatest._
import matchers.should.Matchers._
import org.scalatest.matchers.Matcher
import redis.clients.jedis.Jedis

import scala.jdk.CollectionConverters.mapAsScalaMapConverter
import scala.reflect.runtime.universe.TypeTag

case class Row(customer: String, feature1: Int, feature2: Float, eventTimestamp: java.sql.Timestamp)


class OfflinePipelineSpec extends UnitSpec with ForAllTestContainer {

  override val container = GenericContainer("redis:6.0.8", exposedPorts = Seq(6379))

  trait SparkContext {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("Testing")
      .set("spark.redis.host", container.host)
      .set("spark.redis.port", container.mappedPort(6379).toString)

    val sparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
  }

  trait DataGenerator {
    self: SparkContext =>

    def storeFiles[T <: Product : TypeTag](rows: Seq[T]): String = {
      import self.sparkSession.implicits._

      val tempPath = Files.createTempDirectory("test-dir").toFile.toString + "/rows"

      sparkSession.createDataset(rows)
        .withColumn("date", to_date($"eventTimestamp"))
        .write
        .partitionBy("eventTimestamp")
        .save(tempPath)

      tempPath
    }
  }

  trait Scope {
    val rowGenerator = for {
      customer <- Gen.asciiPrintableStr
      feature1 <- Gen.choose(0, 100)
      feature2 <- Gen.choose(0, 1)
      eventTimestamp <- Gen.choose(0, 30 * 24 * 3600).map(DateTime.now().minusSeconds(_))
    }
      yield Row(customer, feature1, feature2, new java.sql.Timestamp(eventTimestamp.getMillis))

    val config = IngestionJobConfig(
      featureTable = FeatureTable(
        name = "test-fs",
        entities = Seq(Field("customer", ValueType.Enum.STRING)),
        features = Seq(
          Field("feature1", ValueType.Enum.INT32),
          Field("feature2", ValueType.Enum.FLOAT),
        ),
        //offline_source = Some(GSSource(tempPath, Map.empty, "eventTimestamp"))
      ),
      startTime = DateTime.now().minusDays(30),
      endTime = DateTime.now()
    )

    def beStoredRow(row: Row) = {
      val m: Matcher[Map[String, Any]] = contain.allElementsOf(Seq(
        "feature1" -> row.feature1,
        "feature2" -> row.feature2,
        "_ts:test-fs" -> Timestamp.newBuilder().setSeconds(row.eventTimestamp.getTime / 1000).build()
      )).matcher

      m compose {
        (_: Map[Array[Byte], Array[Byte]])
          .map(e => (
            new String(e._1),
            if (new String(e._1).startsWith("_ts"))
              Timestamp.parseFrom(e._2) else
              ValueProto.Value.parseFrom(e._2).asScala
          ))
      }
    }
  }


  "Parquet source file" should "be ingested in redis" in new Scope with SparkContext with DataGenerator {
    val rows = Gen.listOfN(100, rowGenerator).sample.get
    val tempPath = storeFiles(rows)
    val configWithOfflineSource = config.copy(featureTable = config.featureTable.copy(offline_source =
      Some(GSSource(tempPath, Map.empty, "eventTimestamp"))))

    OfflinePipeline.createPipeline(sparkSession, configWithOfflineSource)

    val jedis = new Jedis("localhost", container.mappedPort(6379))
    rows.foreach(
      r => {
        val storedValues = jedis.hgetAll(s"customer:${r.customer}".getBytes).asScala.toMap
        storedValues should beStoredRow(r)
        //        val m = contain allElementsOf(Seq(
        //                  "feature1" -> r.feature1,
        //                  "feature2" -> r.feature2,
        //                  "_ts:test-fs" -> r.eventTimestamp
        //                ))
        //
        //        storedValues should m
      }
    )

  }
}
