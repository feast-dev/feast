package feast.ingestion

import feast.ingestion.sources.bq.BigQueryReader
import feast.ingestion.sources.file.FileReader
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.col

object OfflinePipeline extends BasePipeline {
  override def createPipeline(sparkSession: SparkSession, config: IngestionJobConfig): Unit = {
    val input = config.featureTable.offline_source match {
      case Some(source: BQSource) =>
        BigQueryReader.createBatchSource(
          sparkSession.sqlContext,
          source,
          config.startTime,
          config.endTime
        )
      case Some(source: GSSource) =>
        FileReader.createBatchSource(
          sparkSession.sqlContext, source, config.startTime, config.endTime
        )
    }

    val projection = inputProjection(config.featureTable.offline_source.get, config.featureTable.features, config.featureTable.entities)
    input.select(projection: _*)
      .write
      .format("feast.ingestion.stores.redis")
      .option("entity_columns", config.featureTable.entities.map(_.name).mkString(","))
      .option("entity_names", config.featureTable.entities.map(_.name).mkString(","))
      .option("namespace", config.featureTable.name)
      .option("timestamp_column", config.featureTable.offline_source.get.timestampColumn)
      .save()
  }

  private def inputProjection(source: OfflineSource, features: Seq[Field], entities: Seq[Field]): Array[Column] = {
    val mainColumns =
      if (source.mapping.nonEmpty)
        source.mapping
      else features.map(f => (f.name, f.name))

    val timestampColumn = Seq((source.timestampColumn, source.timestampColumn))
    val entitiesColumns = entities.map(e => (e.name, e.name))

    (mainColumns ++ entitiesColumns ++ timestampColumn).map {
      case (alias, source) => col(source).alias(alias)
    }.toArray
  }
}
