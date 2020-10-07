/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package feast.ingestion.stores.redis

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider}

/**
  * Entrypoint to Redis Storage. Implements only `CreatableRelationProvider` since it's only possible write to Redis.
  * Here we parse configuration from spark parameters & provide SparkRedisConfig to `RedisSinkRelation`
  */
class RedisRelationProvider extends CreatableRelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame
  ): BaseRelation = {
    val config   = SparkRedisConfig.parse(parameters)
    val relation = new RedisSinkRelation(sqlContext, config)

    relation.insert(data, overwrite = false)

    relation
  }
}

class DefaultSource extends RedisRelationProvider
