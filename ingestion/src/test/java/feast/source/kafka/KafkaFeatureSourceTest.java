package feast.source.kafka;

import feast.specs.ImportSpecProto.ImportSpec;
import feast.types.FeatureRowProto.FeatureRow;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
public class KafkaFeatureSourceTest {

  @ClassRule
  public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1, true, "TEST_TOPIC");
  @Rule
  public TestPipeline pipeline = TestPipeline.create();
  @Autowired
  private KafkaTemplate<byte[], FeatureRow> template;


  public void send(FeatureRow... rows) {
    for (FeatureRow row : rows) {
      try {
        log.info("Sent: " + template.send("TEST_TOPIC", row).get().toString());
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ExecutionException e) {
        e.printStackTrace();
      }
    }
  }

  @Test
  public void testFoo() throws ExecutionException, InterruptedException {
    String server = embeddedKafka.getEmbeddedKafka().getBrokerAddresses()[0].toString();
    ImportSpec importSpec = ImportSpec.newBuilder().setType("kafka")
        .addEntities("testEntity")
        .putSourceOptions("topics", "TEST_TOPIC")
        .putSourceOptions("server", server)
        .putJobOptions("sample.limit", "1")
        .build();
    FeatureRow row = FeatureRow.newBuilder().setEntityKey("key").build();
    ScheduledExecutorService scheduler =
        Executors.newScheduledThreadPool(1);
    // we keep sending on loop because beam will only start consuming rows that were sent after startup.
    scheduler.scheduleAtFixedRate(() -> send(row), 0, 1, TimeUnit.SECONDS);

    PCollection<FeatureRow> rows = pipeline.apply(new KafkaFeatureSource(importSpec));
    Assert.assertEquals(IsBounded.BOUNDED, rows.isBounded());
    PAssert.that(rows).containsInAnyOrder(row);
    pipeline.run();
  }

  public Producer<byte[], FeatureRow> getProducer() {
    Map<String, Object> producerProps =
        KafkaTestUtils.producerProps(embeddedKafka.getEmbeddedKafka());
    return new DefaultKafkaProducerFactory<>(producerProps, new ByteArraySerializer(),
        new FeatureRowSerializer()).createProducer();
  }


  public static class FeatureRowSerializer implements Serializer<FeatureRow> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, FeatureRow data) {
      return data.toByteArray();
    }

    @Override
    public void close() {

    }
  }

  @Configuration
  static class ContextConfiguration {

    @Bean
    ProducerFactory<byte[], FeatureRow> producerFactory() {
      Map<String, Object> producerProps =
          KafkaTestUtils.producerProps(embeddedKafka.getEmbeddedKafka());

      return new DefaultKafkaProducerFactory<>(
          producerProps, new ByteArraySerializer(), new FeatureRowSerializer());
    }

    @Bean
    KafkaTemplate<byte[], FeatureRow> kafkaTemplate() {
      return new KafkaTemplate<>(producerFactory(), true);
    }
  }
}