package feast.ingestion.validation

import feast.ingestion.FeatureTable
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

class RowValidator(featureTable: FeatureTable) extends Serializable {
  def allEntitiesPresent: Column =
    featureTable.entities.map(e => col(e.name).isNotNull).reduce(_.&&(_))

  def atLeastOneFeatureNotNull: Column =
    featureTable.features.map(f => col(f.name).isNotNull).reduce(_.||(_))

  def checkAll: Column =
    allEntitiesPresent && atLeastOneFeatureNotNull
}

