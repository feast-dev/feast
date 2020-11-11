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

import java.sql.Timestamp

import org.apache.spark.sql.Row
import redis.clients.jedis.{Pipeline, Response}

/**
  * Determine how a Spark row should be serialized and stored on Redis.
  */
trait Persistence[T] {

  /**
    * Persist a Spark row to Redis
    *
    * @param pipeline              Redis pipeline
    * @param key                   Redis key in serialized bytes format
    * @param row                   Row representing the value to be persist
    * @param expiryTimestamp       Expiry timestamp for the row
    */
  def save(
      pipeline: Pipeline,
      key: Array[Byte],
      row: Row,
      expiryTimestamp: Timestamp
  ): Unit

  /**
    * Returns a Redis response, which can be used by `storedTimestamp` and `newExpiryTimestamp` to
    * derive the currently stored event timestamp, and the updated expiry timestamp. This method will
    * be called prior to persisting the row to Redis, so that `RedisSinkRelation` can decide whether
    * the currently stored value should be updated.
    *
    * @param pipeline              Redis pipeline
    * @param key                   Redis key in serialized bytes format
    * @return                      Redis response representing the row value
    */
  def get(
      pipeline: Pipeline,
      key: Array[Byte]
  ): Response[T]

  /**
    * Returns the currently stored event timestamp for the key and the feature table associated with the ingestion job.
    *
    * @param value              Response returned from `get`
    * @return                   Stored event timestamp associated with the key. Returns `None` if
    *                           the key is not present in Redis, or if timestamp information is
    *                           unavailable on the stored value.
    */
  def storedTimestamp(value: T): Option[Timestamp]

  /**
    * Compute the new expiry timestamp, based on the currently stored value and the new row
    *
    * @param row                Row representing the value to be persist
    * @param value              Response returned from `get`
    * @return                   Expiry timestamp for the new row to be persisted. Will be used
    *                           to compute ttl for the Redis key
    */
  def newExpiryTimestamp(row: Row, value: T): Timestamp
}
