package feast.ingestion.stores.redis

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider}

class RedisRelationProvider extends RelationProvider with CreatableRelationProvider {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = ???

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val config = SparkRedisConfig.parse(parameters)
    val relation = new RedisSinkRelation(sqlContext, config)

    relation.insert(data, overwrite = false)

    relation
  }
}


class DefaultSource extends RedisRelationProvider