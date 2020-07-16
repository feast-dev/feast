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
package feast.core.config;

import feast.core.config.FeastProperties.StreamProperties;
import feast.core.model.Source;
import feast.core.util.KafkaSerialization;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.IngestionJobProto;
import feast.proto.core.SourceProto;
import feast.proto.core.SourceProto.KafkaSourceConfig;
import feast.proto.core.SourceProto.SourceType;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.*;

@Slf4j
@Configuration
public class FeatureStreamConfig {

  String DEFAULT_KAFKA_REQUEST_TIMEOUT_MS_CONFIG = "15000";
  int DEFAULT_SPECS_TOPIC_PARTITIONING = 1;
  short DEFAULT_SPECS_TOPIC_REPLICATION = 3;

  @Bean
  public KafkaAdmin admin(FeastProperties feastProperties) {
    String bootstrapServers = feastProperties.getStream().getOptions().getBootstrapServers();

    Map<String, Object> configs = new HashMap<>();
    configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    configs.put(
        AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, DEFAULT_KAFKA_REQUEST_TIMEOUT_MS_CONFIG);
    return new KafkaAdmin(configs);
  }

  @Bean
  public NewTopic featureRowsTopic(FeastProperties feastProperties) {
    StreamProperties streamProperties = feastProperties.getStream();

    return new NewTopic(
        streamProperties.getOptions().getTopic(),
        streamProperties.getOptions().getPartitions(),
        streamProperties.getOptions().getReplicationFactor());
  }

  @Bean
  public NewTopic featureSetSpecsTopic(FeastProperties feastProperties) {
    StreamProperties streamProperties = feastProperties.getStream();
    Map<String, String> configs = new HashMap<>();
    configs.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);

    NewTopic topic =
        new NewTopic(
            streamProperties.getSpecsOptions().getSpecsTopic(),
            DEFAULT_SPECS_TOPIC_PARTITIONING,
            DEFAULT_SPECS_TOPIC_REPLICATION);

    topic.configs(configs);
    return topic;
  }

  @Bean
  public NewTopic featureSetSpecsAckTopic(FeastProperties feastProperties) {
    StreamProperties streamProperties = feastProperties.getStream();

    return new NewTopic(
        streamProperties.getSpecsOptions().getSpecsAckTopic(),
        DEFAULT_SPECS_TOPIC_PARTITIONING,
        (short) 1);
  }

  @Bean
  public KafkaTemplate<String, FeatureSetProto.FeatureSetSpec> specKafkaTemplate(
      FeastProperties feastProperties) {
    StreamProperties streamProperties = feastProperties.getStream();
    Map<String, Object> props = new HashMap<>();

    props.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        streamProperties.getOptions().getBootstrapServers());

    KafkaTemplate<String, FeatureSetProto.FeatureSetSpec> t =
        new KafkaTemplate<>(
            new DefaultKafkaProducerFactory<>(
                props, new StringSerializer(), new KafkaSerialization.ProtoSerializer<>()));
    t.setDefaultTopic(streamProperties.getSpecsOptions().getSpecsTopic());
    return t;
  }

  @Bean
  public ConsumerFactory<?, ?> ackConsumerFactory(FeastProperties feastProperties) {
    StreamProperties streamProperties = feastProperties.getStream();
    Map<String, Object> props = new HashMap<>();

    props.put(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        streamProperties.getOptions().getBootstrapServers());
    props.put(
        ConsumerConfig.GROUP_ID_CONFIG,
        String.format(
            "core-service-%s", FeatureStreamConfig.class.getPackage().getImplementationVersion()));

    return new DefaultKafkaConsumerFactory<>(
        props,
        new StringDeserializer(),
        new KafkaSerialization.ProtoDeserializer<>(IngestionJobProto.FeatureSetSpecAck.parser()));
  }

  @Autowired
  @Bean
  public Source getDefaultSource(FeastProperties feastProperties) {
    StreamProperties streamProperties = feastProperties.getStream();
    SourceType featureStreamType = SourceType.valueOf(streamProperties.getType().toUpperCase());
    switch (featureStreamType) {
      case KAFKA:
        String bootstrapServers = streamProperties.getOptions().getBootstrapServers();
        String topicName = streamProperties.getOptions().getTopic();

        KafkaSourceConfig sourceConfig =
            KafkaSourceConfig.newBuilder()
                .setBootstrapServers(bootstrapServers)
                .setTopic(topicName)
                .build();
        SourceProto.Source source =
            SourceProto.Source.newBuilder()
                .setType(featureStreamType)
                .setKafkaSourceConfig(sourceConfig)
                .build();
        return Source.fromProto(source, true);
      default:
        throw new RuntimeException("Unsupported source stream, only [KAFKA] is supported");
    }
  }
}
