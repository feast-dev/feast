/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
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
package feast.ingestion.transform;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.mapping.Mapper.Option;
import feast.store.serving.cassandra.CassandraMutation;
import java.io.Serializable;
import java.util.Iterator;
import java.util.concurrent.Future;
import org.apache.beam.sdk.io.cassandra.Mapper;

/** A {@link Mapper} that supports writing {@code CassandraMutation}s with the Beam Cassandra IO. */
public class CassandraMutationMapper implements Mapper<CassandraMutation>, Serializable {

  private com.datastax.driver.mapping.Mapper<CassandraMutation> mapper;
  private Boolean tracing;

  CassandraMutationMapper(
      com.datastax.driver.mapping.Mapper<CassandraMutation> mapper, Boolean tracing) {
    this.mapper = mapper;
    this.tracing = tracing;
  }

  @Override
  public Iterator<CassandraMutation> map(ResultSet resultSet) {
    throw new UnsupportedOperationException("Only supports write operations");
  }

  @Override
  public Future<Void> deleteAsync(CassandraMutation entityClass) {
    throw new UnsupportedOperationException("Only supports write operations");
  }

  /**
   * Saves records to Cassandra with: - Cassandra's internal write time set to the timestamp of the
   * record. Cassandra will not override an existing record with the same partition key if the write
   * time is older - Expiration of the record
   *
   * @param entityClass Cassandra's object mapper
   */
  @Override
  public Future<Void> saveAsync(CassandraMutation entityClass) {
    return mapper.saveAsync(
        entityClass,
        Option.timestamp(entityClass.getWriteTime()),
        Option.ttl(entityClass.getTtl()),
        Option.consistencyLevel(ConsistencyLevel.ONE),
        Option.tracing(tracing));
  }
}
