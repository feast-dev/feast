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
package feast.serving.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import feast.proto.core.DataFormatProto.FileFormat;
import feast.proto.core.DataFormatProto.FileFormat.ParquetFormat;
import feast.proto.core.DataFormatProto.StreamFormat;
import feast.proto.core.DataFormatProto.StreamFormat.AvroFormat;
import feast.proto.core.DataFormatProto.StreamFormat.ProtoFormat;
import feast.proto.core.DataSourceProto.DataSource;
import feast.proto.core.DataSourceProto.DataSource.FileOptions;
import feast.proto.core.DataSourceProto.DataSource.KafkaOptions;
import feast.proto.core.DataSourceProto.DataSource.KinesisOptions;
import feast.proto.core.EntityProto;
import feast.proto.core.FeatureProto;
import feast.proto.core.FeatureProto.FeatureSpecV2;
import feast.proto.core.FeatureTableProto.FeatureTableSpec;
import feast.proto.core.StoreProto;
import feast.proto.serving.ServingAPIProto;
import feast.proto.types.ValueProto;
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

  public static Triple<String, String, Boolean> getDefaultSubscription() {
    return defaultSubscription;
  }

  public static String valueToString(ValueProto.Value v) {
    String stringRepr;
    switch (v.getValCase()) {
      case STRING_VAL:
        stringRepr = v.getStringVal();
        break;
      case INT64_VAL:
        stringRepr = String.valueOf(v.getInt64Val());
        break;
      case INT32_VAL:
        stringRepr = String.valueOf(v.getInt32Val());
        break;
      case BYTES_VAL:
        stringRepr = v.getBytesVal().toString();
        break;
      default:
        throw new RuntimeException("Type is not supported to be entity");
    }

    return stringRepr;
  }

  public static StoreProto.Store getDefaultStore() {
    return defaultStore;
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
      case REDIS_CLUSTER:
        StoreProto.Store.RedisClusterConfig redisClusterConfig =
            StoreProto.Store.RedisClusterConfig.newBuilder().build();
        return builder.setRedisClusterConfig(redisClusterConfig).build();
      default:
        throw new RuntimeException("Unrecognized Store type");
    }
  }

  public static EntityProto.EntitySpecV2 createEntitySpecV2(
      String name,
      String description,
      ValueProto.ValueType.Enum valueType,
      Map<String, String> tags) {
    return EntityProto.EntitySpecV2.newBuilder()
        .setName(name)
        .setDescription(description)
        .setValueType(valueType)
        .putAllTags(tags)
        .build();
  }

  public static FeatureProto.FeatureSpecV2 createFeatureSpecV2(
      String name, ValueProto.ValueType.Enum valueType, Map<String, String> tags) {
    return FeatureProto.FeatureSpecV2.newBuilder()
        .setName(name)
        .setValueType(valueType)
        .putAllTags(tags)
        .build();
  }

  // Create a Feature Table spec without DataSources configured.
  public static FeatureTableSpec createFeatureTableSpec(
      String name,
      List<String> entities,
      Map<String, ValueProto.ValueType.Enum> features,
      int maxAgeSecs,
      Map<String, String> tags) {

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
                            .putAllTags(tags)
                            .build())
                .collect(Collectors.toList()))
        .setMaxAge(Duration.newBuilder().setSeconds(3600).build())
        .setBatchSource(
            DataSource.newBuilder()
                .setTimestampField("ts")
                .setType(DataSource.SourceType.BATCH_FILE)
                .setFileOptions(
                    FileOptions.newBuilder()
                        .setFileFormat(
                            FileFormat.newBuilder()
                                .setParquetFormat(ParquetFormat.newBuilder().build())
                                .build())
                        .setUri("/dev/null")
                        .build())
                .build())
        .putAllLabels(tags)
        .build();
  }

  public static FeatureTableSpec createFeatureTableSpec(
      String name,
      List<String> entities,
      ImmutableMap<String, ValueProto.ValueType.Enum> features,
      int maxAgeSecs,
      Map<String, String> tags) {

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
                            .putAllTags(tags)
                            .build())
                .collect(Collectors.toList()))
        .setMaxAge(Duration.newBuilder().setSeconds(maxAgeSecs).build())
        .putAllLabels(tags)
        .build();
  }

  public static DataSource createFileDataSourceSpec(
      String fileURL, String timestampColumn, String datePartitionColumn) {
    return DataSource.newBuilder()
        .setType(DataSource.SourceType.BATCH_FILE)
        .setFileOptions(
            FileOptions.newBuilder().setFileFormat(createParquetFormat()).setUri(fileURL).build())
        .setTimestampField(timestampColumn)
        .setDatePartitionColumn(datePartitionColumn)
        .build();
  }

  public static DataSource createBigQueryDataSourceSpec(
      String bigQueryTable, String timestampColumn, String datePartitionColumn) {
    return DataSource.newBuilder()
        .setType(DataSource.SourceType.BATCH_BIGQUERY)
        .setBigqueryOptions(DataSource.BigQueryOptions.newBuilder().setTable(bigQueryTable).build())
        .setTimestampField(timestampColumn)
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
                .setKafkaBootstrapServers(servers)
                .setMessageFormat(createProtoFormat("class.path"))
                .build())
        .setTimestampField(timestampColumn)
        .build();
  }

  public static ValueProto.Value createEmptyValue() {
    return ValueProto.Value.newBuilder().build();
  }

  public static ValueProto.Value createStrValue(String val) {
    return ValueProto.Value.newBuilder().setStringVal(val).build();
  }

  public static ValueProto.Value createDoubleValue(double value) {
    return ValueProto.Value.newBuilder().setDoubleVal(value).build();
  }

  public static ValueProto.Value createInt32Value(int value) {
    return ValueProto.Value.newBuilder().setInt32Val(value).build();
  }

  public static ValueProto.Value createInt64Value(long value) {
    return ValueProto.Value.newBuilder().setInt64Val(value).build();
  }

  public static ServingAPIProto.FeatureReferenceV2 createFeatureReference(
      String featureTableName, String featureName) {
    return ServingAPIProto.FeatureReferenceV2.newBuilder()
        .setFeatureViewName(featureTableName)
        .setFeatureName(featureName)
        .build();
  }

  public static ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow createEntityRow(
      String entityName, ValueProto.Value entityValue, long seconds) {
    return ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow.newBuilder()
        .setTimestamp(Timestamp.newBuilder().setSeconds(seconds))
        .putFields(entityName, entityValue)
        .build();
  }

  public static ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow createCompoundEntityRow(
      ImmutableMap<String, ValueProto.Value> entityNameValues, long seconds) {
    ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow.Builder entityRow =
        ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow.newBuilder()
            .setTimestamp(Timestamp.newBuilder().setSeconds(seconds));

    entityNameValues.entrySet().stream()
        .forEach(entry -> entityRow.putFields(entry.getKey(), entry.getValue()));

    return entityRow.build();
  }

  public static DataSource createKinesisDataSourceSpec(
      String region, String streamName, String classPath, String timestampColumn) {
    return DataSource.newBuilder()
        .setType(DataSource.SourceType.STREAM_KINESIS)
        .setKinesisOptions(
            KinesisOptions.newBuilder()
                .setRegion("ap-nowhere1")
                .setStreamName("stream")
                .setRecordFormat(createProtoFormat(classPath))
                .build())
        .setTimestampField(timestampColumn)
        .build();
  }

  public static FileFormat createParquetFormat() {
    return FileFormat.newBuilder().setParquetFormat(ParquetFormat.getDefaultInstance()).build();
  }

  public static StreamFormat createAvroFormat(String schemaJSON) {
    return StreamFormat.newBuilder()
        .setAvroFormat(AvroFormat.newBuilder().setSchemaJson(schemaJSON).build())
        .build();
  }

  public static StreamFormat createProtoFormat(String classPath) {
    return StreamFormat.newBuilder()
        .setProtoFormat(ProtoFormat.newBuilder().setClassPath(classPath).build())
        .build();
  }
}
