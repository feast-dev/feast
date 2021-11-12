/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2021 The Feast Authors
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
package feast.storage.connectors.cassandra.retriever;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

public class CassandraSchemaRegistry {
  private final CqlSession session;
  private final PreparedStatement preparedStatement;
  private final LoadingCache<SchemaReference, GenericDatumReader<GenericRecord>> cache;

  private static String SCHEMA_REF_TABLE = "feast_schema_reference";
  private static String SCHEMA_REF_COLUMN = "schema_ref";
  private static String SCHEMA_COLUMN = "avro_schema";

  public static class SchemaReference {
    private final ByteBuffer schemaHash;

    public SchemaReference(ByteBuffer schemaHash) {
      this.schemaHash = schemaHash;
    }

    public ByteBuffer getSchemaHash() {
      return schemaHash;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SchemaReference that = (SchemaReference) o;
      return Objects.equals(schemaHash, that.schemaHash);
    }

    @Override
    public int hashCode() {
      return Objects.hash(schemaHash);
    }
  }

  public CassandraSchemaRegistry(CqlSession session) {
    this.session = session;
    String tableName = String.format("\"%s\"", SCHEMA_REF_TABLE);
    Select query =
        QueryBuilder.selectFrom(tableName)
            .column(SCHEMA_COLUMN)
            .whereColumn(SCHEMA_REF_COLUMN)
            .isEqualTo(QueryBuilder.bindMarker());
    this.preparedStatement = session.prepare(query.build());

    CacheLoader<SchemaReference, GenericDatumReader<GenericRecord>> schemaCacheLoader =
        CacheLoader.from(this::loadReader);

    cache = CacheBuilder.newBuilder().build(schemaCacheLoader);
  }

  public GenericDatumReader<GenericRecord> getReader(SchemaReference reference) {
    GenericDatumReader<GenericRecord> reader;
    try {
      reader = this.cache.get(reference);
    } catch (ExecutionException | CacheLoader.InvalidCacheLoadException e) {
      throw new RuntimeException("Unable to find Schema");
    }
    return reader;
  }

  private GenericDatumReader<GenericRecord> loadReader(SchemaReference reference) {
    BoundStatement statement = preparedStatement.bind(reference.getSchemaHash());

    Row row = session.execute(statement).one();

    Schema schema =
        new Schema.Parser()
            .parse(StandardCharsets.UTF_8.decode(row.getByteBuffer(SCHEMA_COLUMN)).toString());
    return new GenericDatumReader<>(schema);
  }
}
