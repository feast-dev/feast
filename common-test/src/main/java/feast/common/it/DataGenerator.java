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
package feast.common.it;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import feast.proto.core.DataSourceProto.DataFormat;
import feast.proto.core.DataSourceProto.DataFormat.ParquetFormat;
import feast.proto.core.DataSourceProto.DataFormat.ProtoFormat;
import feast.proto.core.DataSourceProto.DataSource;
import feast.proto.core.DataSourceProto.DataSource.BigQueryOptions;
import feast.proto.core.DataSourceProto.DataSource.FileOptions;
import feast.proto.core.DataSourceProto.DataSource.KafkaOptions;
import feast.proto.core.EntityProto;
import feast.proto.core.FeatureProto;
import feast.proto.core.FeatureProto.FeatureSpecV2;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.FeatureTableProto.FeatureTableSpec;
import feast.proto.core.SourceProto;
import feast.proto.core.StoreProto;
import feast.proto.serving.ServingAPIProto;
import feast.proto.types.ValueProto;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Triple;

public class DataGenerator {
  // projectName, featureName, exclude
  static Triple<String, String, Boolean> defaultSubscription = Triple.of("*", "*", false);

  static StoreProto.Store defaultStore =
      createStore(
          "test-store", StoreProto.Store.StoreType.REDIS, ImmutableList.of(defaultSubscription));

  static SourceProto.Source defaultSource = createSource("localhost", "topic");

  public static Triple<String, String, Boolean> getDefaultSubscription() {
    return defaultSubscription;
  }

  public static StoreProto.Store getDefaultStore() {
    return defaultStore;
  }

  public static SourceProto.Source getDefaultSource() {
    return defaultSource;
  }

  public static FeatureSetProto.FeatureSet getDefaultFeatureSet() {
    return createFeatureSet(DataGenerator.getDefaultSource(), "default", "test");
  }

  public static SourceProto.Source createSource(String server, String topic) {
    return SourceProto.Source.newBuilder()
        .setType(SourceProto.SourceType.KAFKA)
        .setKafkaSourceConfig(
            SourceProto.KafkaSourceConfig.newBuilder()
                .setBootstrapServers(server)
                .setTopic(topic)
                .build())
        .build();
  }

  public static StoreProto.Store createStore(
      String name,
      StoreProto.Store.StoreType type,
      List<Triple<String, String, Boolean>> subscriptions) {
    StoreProto.Store.Builder builder =
        StoreProto.Store.newBuilder()
            .addAllSubscriptions(
                subscriptions.stream()
                    .map(
                        s ->
                            StoreProto.Store.Subscription.newBuilder()
                                .setProject(s.getLeft())
                                .setName(s.getMiddle())
                                .setExclude(s.getRight())
                                .build())
                    .collect(Collectors.toList()))
            .setName(name)
            .setType(type);

    switch (type) {
      case REDIS:
        StoreProto.Store.RedisConfig redisConfig =
            StoreProto.Store.RedisConfig.newBuilder().build();
        return builder.setRedisConfig(redisConfig).build();
      case BIGQUERY:
        StoreProto.Store.BigQueryConfig bqConfig =
            StoreProto.Store.BigQueryConfig.newBuilder().build();
        return builder.setBigqueryConfig(bqConfig).build();
      case REDIS_CLUSTER:
        StoreProto.Store.RedisClusterConfig redisClusterConfig =
            StoreProto.Store.RedisClusterConfig.newBuilder().build();
        return builder.setRedisClusterConfig(redisClusterConfig).build();
      default:
        throw new RuntimeException("Unrecognized Store type");
    }
  }

  public static FeatureSetProto.FeatureSpec createFeature(
      String name, ValueProto.ValueType.Enum valueType, Map<String, String> labels) {
    return FeatureSetProto.FeatureSpec.newBuilder()
        .setName(name)
        .setValueType(valueType)
        .putAllLabels(labels)
        .build();
  }

  public static FeatureSetProto.EntitySpec createEntitySpec(
      String name, ValueProto.ValueType.Enum valueType) {
    return FeatureSetProto.EntitySpec.newBuilder().setName(name).setValueType(valueType).build();
  }

  public static EntityProto.EntitySpecV2 createEntitySpecV2(
      String name,
      String description,
      ValueProto.ValueType.Enum valueType,
      Map<String, String> labels) {
    return EntityProto.EntitySpecV2.newBuilder()
        .setName(name)
        .setDescription(description)
        .setValueType(valueType)
        .putAllLabels(labels)
        .build();
  }

  public static FeatureProto.FeatureSpecV2 createFeatureSpecV2(
      String name, ValueProto.ValueType.Enum valueType, Map<String, String> labels) {
    return FeatureProto.FeatureSpecV2.newBuilder()
        .setName(name)
        .setValueType(valueType)
        .putAllLabels(labels)
        .build();
  }

  public static FeatureSetProto.FeatureSet createFeatureSet(
      SourceProto.Source source,
      String projectName,
      String name,
      List<FeatureSetProto.EntitySpec> entities,
      List<FeatureSetProto.FeatureSpec> features,
      Map<String, String> labels) {
    return FeatureSetProto.FeatureSet.newBuilder()
        .setSpec(
            FeatureSetProto.FeatureSetSpec.newBuilder()
                .setSource(source)
                .setName(name)
                .setProject(projectName)
                .putAllLabels(labels)
                .addAllEntities(entities)
                .addAllFeatures(features)
                .build())
        .build();
  }

  public static FeatureSetProto.FeatureSet createFeatureSet(
      SourceProto.Source source,
      String projectName,
      String name,
      Map<String, ValueProto.ValueType.Enum> entities,
      Map<String, ValueProto.ValueType.Enum> features,
      Map<String, String> labels) {
    return FeatureSetProto.FeatureSet.newBuilder()
        .setSpec(
            FeatureSetProto.FeatureSetSpec.newBuilder()
                .setSource(source)
                .setName(name)
                .setProject(projectName)
                .putAllLabels(labels)
                .addAllEntities(
                    entities.entrySet().stream()
                        .map(entry -> createEntitySpec(entry.getKey(), entry.getValue()))
                        .collect(Collectors.toList()))
                .addAllFeatures(
                    features.entrySet().stream()
                        .map(
                            entry ->
                                createFeature(
                                    entry.getKey(), entry.getValue(), Collections.emptyMap()))
                        .collect(Collectors.toList()))
                .build())
        .build();
  }

  public static FeatureSetProto.FeatureSet createFeatureSet(
      SourceProto.Source source,
      String projectName,
      String name,
      Map<String, ValueProto.ValueType.Enum> entities,
      Map<String, ValueProto.ValueType.Enum> features) {
    return createFeatureSet(source, projectName, name, entities, features, new HashMap<>());
  }

  public static FeatureSetProto.FeatureSet createFeatureSet(
      SourceProto.Source source, String projectName, String name) {
    return createFeatureSet(
        source, projectName, name, Collections.emptyMap(), Collections.emptyMap());
  }

  // Create a Feature Table spec without DataSources configured.
  public static FeatureTableSpec createFeatureTableSpec(
      String name,
      List<String> entities,
      Map<String, ValueProto.ValueType.Enum> features,
      int maxAgeSecs,
      Map<String, String> labels) {

    return FeatureTableSpec.newBuilder()
        .setName(name)
        .addAllEntities(entities)
        .addAllFeatures(
            features.entrySet().stream()
                .map(
                    entry ->
                        FeatureSpecV2.newBuilder()
                            .setName(entry.getKey())
                            .setValueType(entry.getValue())
                            .putAllLabels(labels)
                            .build())
                .collect(Collectors.toList()))
        .setMaxAge(Duration.newBuilder().setSeconds(3600).build())
        .putAllLabels(labels)
        .build();
  }

  public static FeatureTableSpec createFeatureTableSpec(
      String name,
      List<String> entities,
      ImmutableMap<String, ValueProto.ValueType.Enum> features,
      int maxAgeSecs,
      Map<String, String> labels) {

    return FeatureTableSpec.newBuilder()
        .setName(name)
        .addAllEntities(entities)
        .addAllFeatures(
            features.entrySet().stream()
                .map(
                    entry ->
                        FeatureSpecV2.newBuilder()
                            .setName(entry.getKey())
                            .setValueType(entry.getValue())
                            .putAllLabels(labels)
                            .build())
                .collect(Collectors.toList()))
        .setMaxAge(Duration.newBuilder().setSeconds(maxAgeSecs).build())
        .putAllLabels(labels)
        .build();
  }

  public static DataSource createFileDataSourceSpec(
      String fileURL, String fileFormat, String timestampColumn, String datePartitionColumn) {
    return DataSource.newBuilder()
        .setType(DataSource.SourceType.BATCH_FILE)
        .setFileOptions(
            FileOptions.newBuilder()
                .setFileFormat(
                    DataFormat.newBuilder()
                        .setParquetFormat(ParquetFormat.getDefaultInstance())
                        .build())
                .setFileUrl(fileURL)
                .build())
        .setEventTimestampColumn(timestampColumn)
        .setDatePartitionColumn(datePartitionColumn)
        .build();
  }

  public static DataSource createBigQueryDataSourceSpec(
      String bigQueryTableRef, String timestampColumn, String datePartitionColumn) {
    return DataSource.newBuilder()
        .setType(DataSource.SourceType.BATCH_BIGQUERY)
        .setBigqueryOptions(BigQueryOptions.newBuilder().setTableRef(bigQueryTableRef).build())
        .setEventTimestampColumn(timestampColumn)
        .setDatePartitionColumn(datePartitionColumn)
        .build();
  }

  public static DataSource createKafkaDataSourceSpec(
      String servers, String topic, String classPath, String timestampColumn) {
    return DataSource.newBuilder()
        .setType(DataSource.SourceType.STREAM_KAFKA)
        .setKafkaOptions(
            KafkaOptions.newBuilder()
                .setTopic(topic)
                .setBootstrapServers(servers)
                .setMessageFormat(
                    DataFormat.newBuilder()
                        .setProtoFormat(ProtoFormat.newBuilder().setClassPath(classPath).build())
                        .build())
                .build())
        .setEventTimestampColumn(timestampColumn)
        .build();
  }

  public static ValueProto.Value createEmptyValue() {
    return ValueProto.Value.newBuilder().build();
  }

  public static ValueProto.Value createDoubleValue(double value) {
    return ValueProto.Value.newBuilder().setDoubleVal(value).build();
  }

  public static ValueProto.Value createInt64Value(long value) {
    return ValueProto.Value.newBuilder().setInt64Val(value).build();
  }

  public static ServingAPIProto.FeatureReferenceV2 createFeatureReference(
      String featureTableName, String featureName) {
    return ServingAPIProto.FeatureReferenceV2.newBuilder()
        .setFeatureTable(featureTableName)
        .setName(featureName)
        .build();
  }

  public static ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow createEntityRow(
      String entityName, ValueProto.Value entityValue, long seconds) {
    return ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow.newBuilder()
        .setTimestamp(Timestamp.newBuilder().setSeconds(seconds))
        .putFields(entityName, entityValue)
        .build();
  }
}
