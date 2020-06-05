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
package feast.spark.ingestion;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

import com.google.protobuf.Timestamp;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FieldProto.Field;
import feast.proto.types.ValueProto.Value;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.Assume;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;

public class SparkIngestionTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(SparkIngestionTest.class.getName());

  private static final String TOPIC = "spring";

  @ClassRule public static KafkaEmbedded kafka = new KafkaEmbedded(1, false, TOPIC);

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @Rule public final SparkSessionRule spark = new SparkSessionRule();

  @Before
  public void beforeMethod() {
    // Spark 2 only runs on Java 8. Skip tests on Java 11.
    Assume.assumeThat(System.getProperty("java.version"), startsWith("1.8"));
  }

  @Test
  public void streamingQueryShouldWriteKafkaPayloadAsDeltaLake() throws Exception {
    String brokers = kafka.getBrokersAsString();

    Map<String, Object> senderProps = KafkaTestUtils.producerProps(kafka);
    senderProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

    FeatureRow rowSentBeforeQuery = createRow("feature_set1_ref");
    FeatureRow rowSentAfterQueryStart = createRow("feature_set2_ref");
    assertThat(rowSentBeforeQuery, not(rowSentAfterQueryStart));

    Path deltaPath = folder.getRoot().toPath().resolve("delta");
    Dataset<Row> data = null;
    try (KafkaProducer<Void, byte[]> producer = new KafkaProducer<>(senderProps)) {

      producer.send(new ProducerRecord<Void, byte[]>(TOPIC, rowSentBeforeQuery.toByteArray()));
      StreamingQuery query =
          new SparkIngestion(new String[] {brokers, TOPIC, "delta", deltaPath.toString()})
              .createQuery();
      try {
        for (int i = 0; i < 60; i++) {
          producer.send(
              new ProducerRecord<Void, byte[]>(TOPIC, rowSentAfterQueryStart.toByteArray()));
          if (Files.exists(deltaPath)) {
            data = spark.session.read().format("delta").load(deltaPath.toString());
            long count = data.count();
            LOGGER.info("Delta directory contains {} records.", count);
            if (count > 0) {
              break;
            }
          } else {
            LOGGER.info("Delta directory not yet created.");
          }
          Thread.sleep(1000);
        }
      } finally {
        query.stop();
      }
    }

    describedAs("Should have returned data", notNullValue(), data);
    describedAs("Should have returned data", greaterThan(0L), data.count());

    byte[] firstValue = data.select("value").first().getAs(0);
    FeatureRow firstRow = FeatureRow.parseFrom(firstValue);
    assertThat(firstRow, equalTo(rowSentAfterQueryStart));
  }

  private FeatureRow createRow(String featureSet) {
    return FeatureRow.newBuilder()
        .setFeatureSet(featureSet)
        .setEventTimestamp(Timestamp.newBuilder().setNanos(1000))
        .addFields(
            Field.newBuilder().setName("feature1").setValue(Value.newBuilder().setInt32Val(2)))
        .addFields(
            Field.newBuilder().setName("feature2").setValue(Value.newBuilder().setFloatVal(1.0f)))
        .build();
  }

  public final class SparkSessionRule implements TestRule {
    public SparkSession session;

    @Override
    public Statement apply(final Statement base, final Description description) {
      return new Statement() {
        @Override
        public void evaluate() throws Throwable {
          session =
              SparkSession.builder().appName(getClass().getName()).master("local").getOrCreate();
          try {
            base.evaluate(); // This will run the test.
          } finally {
            session.close();
          }
        }
      };
    }
  }
}
