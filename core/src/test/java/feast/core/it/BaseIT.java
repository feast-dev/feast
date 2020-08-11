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
package feast.core.it;

import feast.core.config.FeastProperties;
import feast.core.util.KafkaSerialization;
import feast.proto.core.IngestionJobProto;
import io.prometheus.client.CollectorRegistry;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Table;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.hibernate.engine.spi.SessionImplementor;
import org.junit.jupiter.api.*;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
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
      cleanTables(entityManager);
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
      props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");

      return new DefaultKafkaConsumerFactory<>(
          props, new StringDeserializer(), new ByteArrayDeserializer());
    }

    @Bean
    public KafkaTemplate<String, IngestionJobProto.FeatureSetSpecAck> specAckKafkaTemplate(
        FeastProperties feastProperties) {
      FeastProperties.StreamProperties streamProperties = feastProperties.getStream();
      Map<String, Object> props = new HashMap<>();

      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());

      KafkaTemplate<String, IngestionJobProto.FeatureSetSpecAck> t =
          new KafkaTemplate<>(
              new DefaultKafkaProducerFactory<>(
                  props, new StringSerializer(), new KafkaSerialization.ProtoSerializer<>()));
      t.setDefaultTopic(streamProperties.getSpecsOptions().getSpecsAckTopic());
      return t;
    }
  }

  /**
   * Truncates all tables in Database (between tests or flows). Retries on deadlock
   *
   * @param em EntityManager
   * @throws SQLException
   */
  public static void cleanTables(EntityManager em) throws SQLException {
    List<String> tableNames =
        em.getMetamodel().getEntities().stream()
            .map(e -> e.getJavaType().getAnnotation(Table.class).name())
            .collect(Collectors.toList());

    // this trick needed to get EntityManager with Transaction
    // and we don't want to wrap whole class into @Transactional
    em = em.getEntityManagerFactory().createEntityManager();
    // Transaction needed only once to do unwrap
    SessionImplementor session = em.unwrap(SessionImplementor.class);

    // and here we're actually don't want any transactions
    // but instead we pulling raw connection
    // to be able to retry query if needed
    // since retrying rollbacked transaction is not that easy
    Connection connection = session.connection();

    // retries are needed since truncate require exclusive lock
    // and that often leads to Deadlock
    // since SpringApp is still running in another thread
    var num_retries = 5;
    for (var i = 1; i <= num_retries; i++) {
      try {
        Statement statement = connection.createStatement();
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

  @PersistenceContext EntityManager entityManager;

  /** Used to determine SequentialFlows */
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
      cleanTables(entityManager);
    }
  }
}
