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

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;
import feast.core.config.StorageConfig.StorageSpecs;
import feast.core.dao.EntityInfoRepository;
import feast.core.dao.FeatureGroupInfoRepository;
import feast.core.dao.FeatureInfoRepository;
import feast.core.storage.BigQueryViewTemplater;
import feast.core.storage.SchemaManager;
import feast.core.stream.FeatureStream;
import feast.core.stream.kafka.KafkaFeatureStream;
import feast.core.stream.kafka.KafkaFeatureStreamConfig;
import feast.core.validators.SpecValidator;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import javax.naming.ConfigurationException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

/**
 * Configuration providing utility objects for the core application.
 */
@Configuration
public class ServerUtilConfig {


  @Autowired
  private StorageSpecs storageSpecs;

  /**
   * Get a BigQuery view templater.
   *
   * @return BigQueryViewTemplater
   */
  @Bean
  public BigQueryViewTemplater bigQueryViewTemplater() throws IOException {
    Resource resource = new ClassPathResource("templates/bq_view.tmpl");
    InputStream resourceInputStream = resource.getInputStream();
    String tmpl = CharStreams.toString(new InputStreamReader(resourceInputStream, Charsets.UTF_8));
    return new BigQueryViewTemplater(tmpl);
  }


  /**
   * Get the storage schema manager.
   *
   * @return SchemaManager
   */
  @Bean
  public SchemaManager schemaManager(BigQueryViewTemplater bigQueryViewTemplater) {
    return new SchemaManager(bigQueryViewTemplater, storageSpecs);

  }

  /**
   * Get a spec validator.
   *
   * @return SpecValidator
   */
  @Bean
  public SpecValidator specValidator(
      EntityInfoRepository entityInfoRepository,
      FeatureGroupInfoRepository featureGroupInfoRepository,
      FeatureInfoRepository featureInfoRepository) {
    SpecValidator specValidator =
        new SpecValidator(
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    return specValidator;
  }

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
