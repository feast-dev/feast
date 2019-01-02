package feast.ingestion.deserializer;

import com.google.protobuf.MessageLite;
import feast.types.FeatureRowProto.FeatureRow;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.concurrent.ListenableFuture;

@RunWith(SpringRunner.class)
@EmbeddedKafka(controlledShutdown = true)
public class KafkaFeatureRowDeserializerTest {

  @Autowired private EmbeddedKafkaBroker embeddedKafka;
  @Autowired private KafkaTemplate<byte[], byte[]> template;

  private <MessageType extends MessageLite> void deserialize(MessageType input) {
    // generate a random UUID to create a unique topic and consumer group id for each test
    String uuid = UUID.randomUUID().toString();
    String topic = "topic-" + uuid;

    embeddedKafka.addTopics(topic);

    Deserializer deserializer = new FeatureRowDeserializer();

    Map<String, Object> consumerProps =
        KafkaTestUtils.consumerProps(uuid, Boolean.FALSE.toString(), embeddedKafka);
    ConsumerFactory<FeatureRow, FeatureRow> consumerFactory =
        new DefaultKafkaConsumerFactory<>(consumerProps, deserializer, deserializer);

    BlockingQueue<ConsumerRecord<FeatureRow, FeatureRow>> records = new LinkedBlockingQueue<>();
    ContainerProperties containerProps = new ContainerProperties(topic);
    containerProps.setMessageListener((MessageListener<FeatureRow, FeatureRow>) records::add);

    MessageListenerContainer container =
        new KafkaMessageListenerContainer<>(consumerFactory, containerProps);
    container.start();
    ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());

    byte[] data = input.toByteArray();
    ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(topic, data, data);
    ListenableFuture<SendResult<byte[], byte[]>> producerFuture = template.send(producerRecord);

    try {
      producerFuture.get();
    } catch (InterruptedException e) {
      return;
    } catch (ExecutionException e) {
      throw new KafkaException("Error sending message to Kafka.", e.getCause());
    }

    ConsumerRecord<FeatureRow, FeatureRow> consumerRecord;
    try {
      consumerRecord = records.take();
    } catch (InterruptedException e) {
      return;
    }

    FeatureRow key = consumerRecord.key();
    Assert.assertEquals(key, input);

    FeatureRow value = consumerRecord.value();
    Assert.assertEquals(value, input);
  }

  @Test(timeout = 10000)
  public void deserializeFeatureRowProto() {
    FeatureRow message = FeatureRow.newBuilder().setEntityName("test").build();
    deserialize(message);
  }

  @Configuration
  static class ContextConfiguration {

    @Autowired private EmbeddedKafkaBroker embeddedKafka;

    @Bean
    ProducerFactory<byte[], byte[]> producerFactory() {
      Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);

      return new DefaultKafkaProducerFactory<>(
          producerProps, new ByteArraySerializer(), new ByteArraySerializer());
    }

    @Bean
    KafkaTemplate<byte[], byte[]> kafkaTemplate() {
      return new KafkaTemplate<>(producerFactory(), true);
    }
  }
}
