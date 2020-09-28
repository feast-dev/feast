package feast.ingestion.sources.file

import java.sql.Timestamp

import feast.ingestion.GSSource
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.joda.time.DateTime

object FileReader {
  def createBatchSource(sqlContext: SQLContext, source: GSSource,
                        start: DateTime, end: DateTime): DataFrame = {
    sqlContext.read
      .parquet(source.path)
      .filter(col(source.timestampColumn) >= new Timestamp(start.getMillis))
      .filter(col(source.timestampColumn) < new Timestamp(end.getMillis))
  }
}
