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

import io.prometheus.client.CollectorRegistry;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Base Integration Test class. Setups postgres and kafka containers. Configures related properties
 * and beans. Provides DB related clean up between tests.
 */
@SpringBootTest
@ActiveProfiles("it")
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class BaseIT {

  @Container public static PostgreSQLContainer<?> postgreSQLContainer = new PostgreSQLContainer<>();

  @Container public static KafkaContainer kafka = new KafkaContainer();

  /**
   * Configure Spring Application to use postgres and kafka rolled out in containers
   *
   * @param registry
   */
  @DynamicPropertySource
  static void properties(DynamicPropertyRegistry registry) {

    registry.add("spring.datasource.url", postgreSQLContainer::getJdbcUrl);
    registry.add("spring.datasource.username", postgreSQLContainer::getUsername);
    registry.add("spring.datasource.password", postgreSQLContainer::getPassword);
    registry.add("spring.jpa.hibernate.ddl-auto", () -> "none");

    registry.add("feast.stream.options.bootstrapServers", kafka::getBootstrapServers);
  }

  /**
   * SequentialFlow is base class that is supposed to be inherited by @Nested test classes that
   * wants to preserve context between test cases. For SequentialFlow databases is being truncated
   * only once after all tests passed.
   */
  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  public class SequentialFlow {
    @AfterAll
    public void tearDown() throws Exception {
      cleanTables();
    }
  }

  /**
   * This class must be inherited inside IT Class and annotated with {@link
   * org.springframework.boot.test.context.TestConfiguration}. It provides configuration needed to
   * communicate with Feast via Kafka
   */
  public static class BaseTestConfig {
    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, byte[]>>
        testListenerContainerFactory(ConsumerFactory<String, byte[]> consumerFactory) {
      ConcurrentKafkaListenerContainerFactory<String, byte[]> factory =
          new ConcurrentKafkaListenerContainerFactory<>();
      factory.setConsumerFactory(consumerFactory);
      return factory;
    }

    @Bean
    public ConsumerFactory<String, byte[]> testConsumerFactory() {
      Map<String, Object> props = new HashMap<>();

      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
      props.put(ConsumerConfig.GROUP_ID_CONFIG, this.getClass().getName());

      return new DefaultKafkaConsumerFactory<>(
          props, new StringDeserializer(), new ByteArrayDeserializer());
    }
  }

  /**
   * Truncates all tables in Database (between tests or flows). Retries on deadlock
   *
   * @throws SQLException when a SQL exception occurs
   */
  public static void cleanTables() throws SQLException {
    Connection connection =
        DriverManager.getConnection(
            postgreSQLContainer.getJdbcUrl(),
            postgreSQLContainer.getUsername(),
            postgreSQLContainer.getPassword());

    List<String> tableNames = new ArrayList<>();
    Statement statement = connection.createStatement();
    ResultSet rs =
        statement.executeQuery(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public'");
    while (rs.next()) {
      tableNames.add(rs.getString(1));
    }

    if (tableNames.isEmpty()) {
      return;
    }

    // retries are needed since truncate require exclusive lock
    // and that often leads to Deadlock
    // since SpringApp is still running in another thread
    int num_retries = 5;
    for (int i = 1; i <= num_retries; i++) {
      try {
        statement = connection.createStatement();
        statement.execute(String.format("truncate %s cascade", String.join(", ", tableNames)));
      } catch (SQLException e) {
        if (i == num_retries) {
          throw e;
        }
        continue;
      }

      break;
    }
  }

  /**
   * Used to determine SequentialFlows
   *
   * @param testInfo test info
   * @return true if test is sequential
   */
  public Boolean isSequentialTest(TestInfo testInfo) {
    try {
      testInfo.getTestClass().get().asSubclass(SequentialFlow.class);
    } catch (ClassCastException e) {
      return false;
    }
    return true;
  }

  @AfterEach
  public void tearDown(TestInfo testInfo) throws Exception {
    CollectorRegistry.defaultRegistry.clear();

    if (!isSequentialTest(testInfo)) {
      cleanTables();
    }
  }
}
