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
import com.google.common.collect.Maps;
import io.grpc.Status;
import static feast.serving.util.Metrics.requestCount;
import static feast.serving.util.Metrics.requestLatency;
import static feast.serving.util.RefUtil.generateFeatureStringRef;

import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.serving.ServingAPIProto.FeatureReference;
import feast.serving.specs.CachedSpecService;
import feast.serving.specs.FeatureSetRequest;
import feast.serving.ServingAPIProto.FeastServingType;
import feast.serving.ServingAPIProto.GetBatchFeaturesRequest;
import feast.serving.ServingAPIProto.GetBatchFeaturesResponse;
import feast.serving.ServingAPIProto.GetFeastServingInfoRequest;
import feast.serving.ServingAPIProto.GetFeastServingInfoResponse;
import feast.serving.ServingAPIProto.GetJobRequest;
import feast.serving.ServingAPIProto.GetJobResponse;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest.EntityRow;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse.FieldValues;
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

public class CassandraServingService implements ServingService {

  private final Session session;
  private final String keyspace;
  private final String tableName;
  private final Tracer tracer;
  private final CachedSpecService specService;

  public CassandraServingService(
      Session session,
      String keyspace,
      String tableName,
      CachedSpecService specService,
      Tracer tracer) {
    this.session = session;
    this.keyspace = keyspace;
    this.tableName = tableName;
    this.tracer = tracer;
    this.specService = specService;
  }

    /** {@inheritDoc} */
  @Override
  public GetFeastServingInfoResponse getFeastServingInfo(
      GetFeastServingInfoRequest getFeastServingInfoRequest) {
    return GetFeastServingInfoResponse.newBuilder()
        .setType(FeastServingType.FEAST_SERVING_TYPE_ONLINE)
        .build();
  }

  /** {@inheritDoc} */
  @Override
  public GetOnlineFeaturesResponse getOnlineFeatures(GetOnlineFeaturesRequest request) {
    try (Scope scope = tracer.buildSpan("Cassandra-getOnlineFeatures").startActive(true)) {
      long startTime = System.currentTimeMillis();
      GetOnlineFeaturesResponse.Builder getOnlineFeaturesResponseBuilder =
          GetOnlineFeaturesResponse.newBuilder();

      List<EntityRow> entityRows = request.getEntityRowsList();
      Map<EntityRow, Map<String, Value>> featureValuesMap =
          entityRows.stream()
              .collect(Collectors.toMap(row -> row, row -> Maps.newHashMap(row.getFieldsMap())));
      List<FeatureSetRequest> featureSetRequests =
          specService.getFeatureSets(request.getFeaturesList());
      for (FeatureSetRequest featureSetRequest : featureSetRequests) {

        List<String> featureSetEntityNames =
            featureSetRequest.getSpec().getEntitiesList().stream()
                .map(EntitySpec::getName)
                .collect(Collectors.toList());

        List<String> cassandraKeys =
            createLookupKeys(featureSetEntityNames, entityRows, featureSetRequest);
        try {
            getAndProcessAll(cassandraKeys, entityRows, featureValuesMap, featureSetRequest);
        } catch (Exception e) {
          throw Status.INTERNAL
              .withDescription("Unable to parse cassandea response/ while retrieving feature")
              .withCause(e)
              .asRuntimeException();
        }
      }
      List<FieldValues> fieldValues =
          featureValuesMap.values().stream()
              .map(valueMap -> FieldValues.newBuilder().putAllFields(valueMap).build())
              .collect(Collectors.toList());
      requestLatency
          .labels("getOnlineFeatures")
          .observe((System.currentTimeMillis() - startTime) / 1000);
      return getOnlineFeaturesResponseBuilder.addAllFieldValues(fieldValues).build();
    }
  }

  @Override
  public GetBatchFeaturesResponse getBatchFeatures(GetBatchFeaturesRequest getFeaturesRequest) {
    throw Status.UNIMPLEMENTED.withDescription("Method not implemented").asRuntimeException();
  }

  @Override
  public GetJobResponse getJob(GetJobRequest getJobRequest) {
    throw Status.UNIMPLEMENTED.withDescription("Method not implemented").asRuntimeException();
  }


  List<String> createLookupKeys(
      List<String> featureSetEntityNames,
      List<EntityRow> entityRows,
      FeatureSetRequest featureSetRequest) {
    try (Scope scope = tracer.buildSpan("Cassandra-makeCassandraKeys").startActive(true)) {
      FeatureSetSpec fsSpec = featureSetRequest.getSpec();
      String featureSetId =
          String.format("%s:%s", fsSpec.getName(), fsSpec.getVersion());
      return entityRows.stream()
          .map(row -> createCassandraKey(featureSetId, featureSetEntityNames, row))
          .collect(Collectors.toList());
    }
  }

  protected boolean isEmpty(ResultSet response) {
    return response.isExhausted();
  }

  /**
   * Send a list of get request as an mget
   *
   * @param keys list of string keys
   *
   */
  protected void getAndProcessAll(
      List<String> keys,
      List<EntityRow> entityRows,
      Map<EntityRow, Map<String, Value>> featureValuesMap,
      FeatureSetRequest featureSetRequest) {
    FeatureSetSpec spec = featureSetRequest.getSpec();
    List<ResultSet> results = new ArrayList<>();
    long startTime = System.currentTimeMillis();
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
    try (Scope scope = tracer.buildSpan("Cassandra-processResponse").startActive(true)) {
      for (int i = 0; i < results.size(); i++) {
        EntityRow entityRow = entityRows.get(i);
        Map<String, Value> featureValues = featureValuesMap.get(entityRow);
        ResultSet queryRows = results.get(i);
        Instant instant = Instant.now();
        List<Field> fields = new ArrayList<>();
        while (queryRows.isExhausted()) {
          Row row = queryRows.one();
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
        FeatureRow featureRow = FeatureRow.newBuilder()
                .addAllFields(fields)
                .setEventTimestamp(
                        Timestamp.newBuilder()
                                .setSeconds(instant.getEpochSecond())
                                .setNanos(instant.getNano())
                                .build())
                .build();
        featureSetRequest
                .getFeatureReferences()
                .parallelStream()
                .forEach(
                        request ->
                                requestCount
                                        .labels(
                                                spec.getProject(),
                                                String.format("%s:%d", request.getName(), request.getVersion()))
                                        .inc());
        Map<String, FeatureReference> featureNames =
                featureSetRequest.getFeatureReferences().stream()
                        .collect(
                                Collectors.toMap(
                                        FeatureReference::getName, featureReference -> featureReference));
        featureRow.getFieldsList().stream()
                .filter(field -> featureNames.keySet().contains(field.getName()))
                .forEach(
                        field -> {
                          FeatureReference ref = featureNames.get(field.getName());
                          String id = generateFeatureStringRef(ref);
                          featureValues.put(id, field.getValue());
                        });
      }
    }
    finally {
      requestLatency
              .labels("processResponse")
              .observe((System.currentTimeMillis() - startTime) / 1000);
    }
  }

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
