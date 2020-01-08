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

import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;
import feast.store.serving.cassandra.CassandraMutation;
import org.apache.beam.sdk.io.cassandra.Mapper;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class CassandraMutationMapperFactory implements SerializableFunction<Session, Mapper> {

  private transient MappingManager mappingManager;
  private Class<CassandraMutation> entityClass;

  public CassandraMutationMapperFactory(Class<CassandraMutation> entityClass) {
    this.entityClass = entityClass;
  }

  @Override
  public Mapper apply(Session session) {
    if (mappingManager == null) {
      this.mappingManager = new MappingManager(session);
    }

    return new CassandraMutationMapper(mappingManager.mapper(entityClass));
  }
}
