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
package feast.core.util;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import feast.common.logging.AuditLogger;
import feast.common.logging.config.LoggingProperties;
import feast.common.logging.config.LoggingProperties.AuditLogProperties;
import feast.core.model.Entity;
import feast.core.model.Feature;
import feast.core.model.FeatureSet;
import feast.core.model.FeatureSetDeliveryStatus;
import feast.core.model.Project;
import feast.core.model.Source;
import feast.core.model.Store;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.FeatureSetProto.FeatureSetMeta;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.SourceProto;
import feast.proto.core.StoreProto;
import feast.proto.core.StoreProto.Store.RedisConfig;
import feast.proto.core.StoreProto.Store.StoreType;
import feast.proto.core.StoreProto.Store.Subscription;
import feast.proto.types.ValueProto;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.springframework.boot.info.BuildProperties;

public class TestUtil {
  public static Set<FeatureSetDeliveryStatus> makeFeatureSetJobStatus(FeatureSet... featureSets) {
    return Stream.of(featureSets)
        .map(
            fs -> {
              FeatureSetDeliveryStatus s = new FeatureSetDeliveryStatus();
              return s;
            })
        .collect(Collectors.toSet());
  }

  public static Set<FeatureSetDeliveryStatus> makeFeatureSetJobStatus(
      List<FeatureSet> featureSets) {
    return makeFeatureSetJobStatus(featureSets.toArray(FeatureSet[]::new));
  }

  public static Source createKafkaSource(String boostrapServers, String topic, boolean isDefault) {
    return Source.fromProto(
        SourceProto.Source.newBuilder()
            .setType(SourceProto.SourceType.KAFKA)
            .setKafkaSourceConfig(
                SourceProto.KafkaSourceConfig.newBuilder()
                    .setBootstrapServers(boostrapServers)
                    .setTopic(topic)
                    .build())
            .build(),
        true);
  }

  public static Source defaultSource = createKafkaSource("kafka:9092", "topic", true);

  public static Store createStore(String name, List<Subscription> subscriptions) {
    return Store.fromProto(
        StoreProto.Store.newBuilder()
            .setName(name)
            .setType(StoreType.REDIS)
            .setRedisConfig(RedisConfig.newBuilder().build())
            .addAllSubscriptions(subscriptions)
            .build());
  }

  public static FeatureSet CreateFeatureSet(
      String name, String project, List<Entity> entities, List<Feature> features) {
    FeatureSet fs =
        new FeatureSet(
            name,
            project,
            100L,
            entities,
            features,
            defaultSource,
            new HashMap<>(),
            FeatureSetProto.FeatureSetStatus.STATUS_READY);
    fs.setVersion(1);
    return fs;
  }

  public static FeatureSet createEmptyFeatureSet(String name, Source source) {
    return FeatureSet.fromProto(
        FeatureSetProto.FeatureSet.newBuilder()
            .setSpec(
                FeatureSetSpec.newBuilder()
                    .setSource(source.toProto())
                    .setProject(Project.DEFAULT_NAME)
                    .setName(name))
            .setMeta(FeatureSetMeta.newBuilder().build())
            .build());
  }

  public static Feature CreateFeature(String name, ValueProto.ValueType.Enum valueType) {
    return CreateFeature(name, valueType, Map.of());
  }

  public static Feature CreateFeature(
      String name, ValueProto.ValueType.Enum valueType, Map<String, String> labels) {
    return Feature.fromProto(
        FeatureSetProto.FeatureSpec.newBuilder()
            .setName(name)
            .setValueType(valueType)
            .putAllLabels(labels)
            .build());
  }

  public static Entity CreateEntity(String name, ValueProto.ValueType.Enum valueType) {
    return Entity.fromProto(
        FeatureSetProto.EntitySpec.newBuilder().setName(name).setValueType(valueType).build());
  }

  /** Setup the audit logger. This call is required to use the audit logger when testing. */
  public static void setupAuditLogger() {
    AuditLogProperties properties = new AuditLogProperties();
    properties.setEnabled(true);
    LoggingProperties loggingProperties = new LoggingProperties();
    loggingProperties.setAudit(properties);

    BuildProperties buildProperties = mock(BuildProperties.class);
    when(buildProperties.getArtifact()).thenReturn("feast-core");
    when(buildProperties.getVersion()).thenReturn("0.6");

    new AuditLogger(loggingProperties, buildProperties);
  }
}
