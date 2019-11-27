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
package feast.serving.service;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import feast.serving.ServingAPIProto.FeatureSetRequest;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest.EntityRow;
import feast.serving.util.ValueUtil;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto.Field;
import feast.types.ValueProto.Value;
import io.opentracing.Scope;
import io.opentracing.Tracer;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CassandraServingService extends OnlineServingService<String, ResultSet> {

  private final Session session;
  private final String keyspace;
  private final String tableName;
  private final Tracer tracer;

  public CassandraServingService(
      Session session,
      String keyspace,
      String tableName,
      CachedSpecService specService,
      Tracer tracer) {
    super(specService, tracer);
    this.session = session;
    this.keyspace = keyspace;
    this.tableName = tableName;
    this.tracer = tracer;
  }

  @Override
  List<String> createLookupKeys(
      List<String> featureSetEntityNames,
      List<EntityRow> entityRows,
      FeatureSetRequest featureSetRequest) {
    try (Scope scope = tracer.buildSpan("Cassandra-makeCassandraKeys").startActive(true)) {
      String featureSetId =
          String.format("%s:%s", featureSetRequest.getName(), featureSetRequest.getVersion());
      return entityRows.stream()
          .map(row -> createCassandraKey(featureSetId, featureSetEntityNames, row))
          .collect(Collectors.toList());
    }
  }

  @Override
  protected boolean isEmpty(ResultSet response) {
    return response.isExhausted();
  }

  /**
   * Send a list of get request as an mget
   *
   * @param keys list of string keys
   * @return list of {@link FeatureRow} in primitive byte representation for each key
   */
  @Override
  protected List<ResultSet> getAll(List<String> keys) {
    List<ResultSet> results = new ArrayList<>();
    for (String key : keys) {
      results.add(
          session.execute(
              QueryBuilder.select()
                  .column("entities")
                  .column("feature")
                  .column("value")
                  .writeTime("value")
                  .as("writetime")
                  .from(keyspace, tableName)
                  .where(QueryBuilder.eq("entities", key))));
    }
    return results;
  }

  @Override
  FeatureRow parseResponse(ResultSet resultSet) {
    List<Field> fields = new ArrayList<>();
    Instant instant = Instant.now();
    while (!resultSet.isExhausted()) {
      Row row = resultSet.one();
      long microSeconds = row.getLong("writetime");
      instant =
          Instant.ofEpochSecond(
              TimeUnit.MICROSECONDS.toSeconds(microSeconds),
              TimeUnit.MICROSECONDS.toNanos(
                  Math.floorMod(microSeconds, TimeUnit.SECONDS.toMicros(1))));
      try {
        fields.add(
            Field.newBuilder()
                .setName(row.getString("feature"))
                .setValue(Value.parseFrom(ByteBuffer.wrap(row.getBytes("value").array())))
                .build());
      } catch (InvalidProtocolBufferException e) {
        e.printStackTrace();
      }
    }
    return FeatureRow.newBuilder()
        .addAllFields(fields)
        .setEventTimestamp(
            Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build())
        .build();
  }

  /**
   * Create cassandra keys
   *
   * @param featureSet featureSet reference of the feature. E.g. feature_set_1:1
   * @param featureSetEntityNames entity names that belong to the featureSet
   * @param entityRow entityRow to build the key from
   * @return String
   */
  private static String createCassandraKey(
      String featureSet, List<String> featureSetEntityNames, EntityRow entityRow) {
    Map<String, Value> fieldsMap = entityRow.getFieldsMap();
    List<String> res = new ArrayList<>();
    for (String entityName : featureSetEntityNames) {
      res.add(entityName + "=" + ValueUtil.toString(fieldsMap.get(entityName)));
    }
    return featureSet + ":" + String.join("|", res);
  }
}
