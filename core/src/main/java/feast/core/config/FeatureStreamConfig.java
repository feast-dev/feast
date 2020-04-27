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

import com.google.common.base.Strings;
import feast.core.SourceProto.KafkaSourceConfig;
import feast.core.SourceProto.SourceType;
import feast.core.config.FeastProperties.StreamProperties;
import feast.core.model.Source;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class FeatureStreamConfig {

  String DEFAULT_KAFKA_REQUEST_TIMEOUT_MS_CONFIG = "15000";

  @Autowired
  @Bean
  public Source getDefaultSource(FeastProperties feastProperties) {
    StreamProperties streamProperties = feastProperties.getStream();
    SourceType featureStreamType = SourceType.valueOf(streamProperties.getType().toUpperCase());
    switch (featureStreamType) {
      case KAFKA:
        String bootstrapServers = streamProperties.getOptions().getBootstrapServers();
        String topicName = streamProperties.getOptions().getTopic();
        Map<String, Object> map = new HashMap<>();
        map.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        map.put(
            AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, DEFAULT_KAFKA_REQUEST_TIMEOUT_MS_CONFIG);
        AdminClient client = AdminClient.create(map);

        NewTopic newTopic =
            new NewTopic(
                topicName,
                streamProperties.getOptions().getPartitions(),
                streamProperties.getOptions().getReplicationFactor());
        CreateTopicsResult createTopicsResult =
            client.createTopics(Collections.singleton(newTopic));
        try {
          createTopicsResult.values().get(topicName).get();
        } catch (InterruptedException | ExecutionException e) {
          if (e.getCause().getClass().equals(TopicExistsException.class)) {
            log.warn(
                Strings.lenientFormat(
                    "Unable to create topic %s in the feature stream, topic already exists, using existing topic.",
                    topicName));
          } else {
            throw new RuntimeException(e.getMessage(), e);
          }
        }
        KafkaSourceConfig sourceConfig =
            KafkaSourceConfig.newBuilder()
                .setBootstrapServers(bootstrapServers)
                .setTopic(topicName)
                .build();
        return new Source(featureStreamType, sourceConfig, true);
      default:
        throw new RuntimeException("Unsupported source stream, only [KAFKA] is supported");
    }
  }
}
