/*
 * Copyright 2018 The Feast Authors
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
 *
 */

package feast.core.config;

import feast.core.stream.FeatureStream;
import feast.core.stream.kafka.KafkaFeatureStream;
import feast.core.stream.kafka.KafkaFeatureStreamConfig;
import java.util.HashMap;
import java.util.Map;
import javax.naming.ConfigurationException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration providing utility objects for the core application.
 */
@Configuration
public class StreamConfig {

  /**
   * Get the featureStream
   * @param type type of stream, e.g. kafka
   * @param options options in the format of a json key-value map
   * @return FeatureStream object
   * @throws ConfigurationException
   */
  @Bean
  public FeatureStream featureStream(@Value("${feast.stream.type}") String type,
      @Value("${feast.stream.options}") String options) throws ConfigurationException {
    switch (type) {
      case "kafka":
        KafkaFeatureStreamConfig config = KafkaFeatureStreamConfig.fromJSON(options);
        Map<String, Object> map = new HashMap<>();
        map.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        map.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000");
        AdminClient client = AdminClient.create(map);
        return new KafkaFeatureStream(client, config);
      default:
        throw new ConfigurationException(
            "Invalid feature stream type set in feast.stream.type. Supported types: [kafka]");
    }
  }
}
