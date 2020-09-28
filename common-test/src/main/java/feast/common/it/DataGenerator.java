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
import com.google.protobuf.Duration;
import feast.proto.core.EntityProto;
import feast.proto.core.FeatureProto.FeatureSpecV2;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.FeatureSourceProto.FeatureSourceSpec;
import feast.proto.core.FeatureSourceProto.FeatureSourceSpec.FileOptions;
import feast.proto.core.FeatureSourceProto.FeatureSourceSpec.FileOptions.FileFormat;
import feast.proto.core.FeatureSourceProto.FeatureSourceSpec.KafkaOptions;
import feast.proto.core.FeatureTableProto.FeatureTableSpec;
import feast.proto.core.SourceProto;
import feast.proto.core.StoreProto;
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

  // Create a Feature Table spec without feature sources configured.
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
        .build();
  }

  public static FeatureSourceSpec createFileFeatureSourceSpec(String fileURL) {
    return FeatureSourceSpec.newBuilder()
        .setType(FeatureSourceSpec.SourceType.BATCH_FILE)
        .setFileOptions(
            FileOptions.newBuilder().setFileFormat(FileFormat.PARQUET).setFileUrl(fileURL).build())
        .build();
  }

  public static FeatureSourceSpec createKafkaFeatureSourceSpec(
      String servers, String topic, String classPath) {
    return FeatureSourceSpec.newBuilder()
        .setType(FeatureSourceSpec.SourceType.STREAM_KAFKA)
        .setKafkaOptions(
            KafkaOptions.newBuilder()
                .setTopic(topic)
                .setBootstrapServers(servers)
                .setClassPath(classPath)
                .build())
        .build();
  }
}
