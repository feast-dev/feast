package feast.ingestion.sources.bq

import java.sql.Timestamp

import feast.ingestion.BQSource
import org.joda.time.DateTime
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.col

object BigQueryReader {
  def createBatchSource(sqlContext: SQLContext, source: BQSource,
                        start: DateTime, end: DateTime): DataFrame = {
    sqlContext.read
      .format("bigquery")
      .load(s"${source.project}.${source.dataset}.${source.table}")
      .filter(col(source.timestampColumn) >= new Timestamp(start.getMillis))
      .filter(col(source.timestampColumn) < new Timestamp(end.getMillis))
  }
}
