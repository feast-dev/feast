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
package feast.test;

import static feast.ingestion.utils.SpecUtil.getFeatureSetReference;

import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import feast.core.FeatureSetProto.FeatureSet;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.ingestion.transform.metrics.WriteSuccessMetricsTransform;
import feast.storage.RedisProto.RedisKey;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FeatureRowProto.FeatureRow.Builder;
import feast.types.FieldProto.Field;
import feast.types.ValueProto.*;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.joda.time.Duration;
import redis.embedded.RedisServer;

@SuppressWarnings("WeakerAccess")
public class TestUtil {

  public static class LocalRedis {

    private static RedisServer server;

    /**
     * Start local Redis for used in testing at "localhost"
     *
     * @param port port number
     * @throws IOException if Redis failed to start
     */
    public static void start(int port) throws IOException {
      server = new RedisServer(port);
      server.start();
    }

    public static void stop() {
      if (server != null) {
        server.stop();
      }
    }
  }

  public static class LocalKafka {

    private static KafkaServerStartable server;

    /**
     * Start local Kafka and (optionally) Zookeeper
     *
     * @param kafkaHost e.g. localhost
     * @param kafkaPort e.g. 60001
     * @param kafkaReplicationFactor e.g. 1
     * @param zookeeperHost e.g. localhost
     * @param zookeeperPort e.g. 60002
     * @param zookeeperDataDir e.g. "/tmp" or "Files.createTempDir().getAbsolutePath()"
     */
    public static void start(
        String kafkaHost,
        int kafkaPort,
        short kafkaReplicationFactor,
        boolean startZookeper,
        String zookeeperHost,
        int zookeeperPort,
        String zookeeperDataDir)
        throws InterruptedException {
      if (startZookeper) {
        LocalZookeeper.start(zookeeperPort, zookeeperDataDir);
        Thread.sleep(5000);
      }
      Properties kafkaProp = new Properties();
      kafkaProp.put("zookeeper.connect", zookeeperHost + ":" + zookeeperPort);
      kafkaProp.put("host.name", kafkaHost);
      kafkaProp.put("port", kafkaPort);
      kafkaProp.put("offsets.topic.replication.factor", kafkaReplicationFactor);
      KafkaConfig kafkaConfig = new KafkaConfig(kafkaProp);
      server = new KafkaServerStartable(kafkaConfig);
      new Thread(server::startup).start();
    }

    public static void stop() {
      if (server != null) {
        try {
          server.shutdown();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }

  /**
   * Publish test Feature Row messages to a running Kafka broker
   *
   * @param bootstrapServers e.g. localhost:9092
   * @param topic e.g. my_topic
   * @param messages e.g. list of Feature Row
   * @param valueSerializer in Feast this valueSerializer should be "ByteArraySerializer.class"
   * @param publishTimeoutSec duration to wait for publish operation (of each message) to succeed
   */
  public static void publishFeatureRowsToKafka(
      String bootstrapServers,
      String topic,
      List<FeatureRow> messages,
      Class<?> valueSerializer,
      long publishTimeoutSec) {
    Long defaultKey = 1L;
    Properties prop = new Properties();
    prop.put("bootstrap.servers", bootstrapServers);
    prop.put("key.serializer", LongSerializer.class);
    prop.put("value.serializer", valueSerializer);
    Producer<Long, byte[]> producer = new KafkaProducer<>(prop);

    messages.forEach(
        featureRow -> {
          ProducerRecord<Long, byte[]> record =
              new ProducerRecord<>(topic, defaultKey, featureRow.toByteArray());
          try {
            producer.send(record).get(publishTimeoutSec, TimeUnit.SECONDS);
          } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
          }
        });
  }

  /**
   * Create a Feature Row with random value according to the FeatureSetSpec
   *
   * <p>See {@link #createRandomFeatureRow(FeatureSetSpec, int)}
   *
   * @param featureSetSpec {@link FeatureSetSpec}
   * @return {@link FeatureRow}
   */
  public static FeatureRow createRandomFeatureRow(FeatureSetSpec featureSetSpec) {
    ThreadLocalRandom random = ThreadLocalRandom.current();
    int randomStringSizeMaxSize = 12;
    return createRandomFeatureRow(featureSetSpec, random.nextInt(0, randomStringSizeMaxSize) + 4);
  }

  /**
   * Create a Feature Row with random value according to the FeatureSet.
   *
   * <p>The Feature Row created contains fields according to the entities and features defined in
   * FeatureSet, matching the value type of the field, with randomized value for testing.
   *
   * @param featureSetSpec {@link FeatureSetSpec}
   * @param randomStringSize number of characters for the generated random string
   * @return {@link FeatureRow}
   */
  public static FeatureRow createRandomFeatureRow(
      FeatureSetSpec featureSetSpec, int randomStringSize) {
    Builder builder =
        FeatureRow.newBuilder()
            .setFeatureSet(getFeatureSetReference(featureSetSpec))
            .setEventTimestamp(Timestamps.fromMillis(System.currentTimeMillis()));

    featureSetSpec
        .getEntitiesList()
        .forEach(
            field -> {
              builder.addFields(
                  Field.newBuilder()
                      .setName(field.getName())
                      .setValue(createRandomValue(field.getValueType(), randomStringSize))
                      .build());
            });

    featureSetSpec
        .getFeaturesList()
        .forEach(
            field -> {
              builder.addFields(
                  Field.newBuilder()
                      .setName(field.getName())
                      .setValue(createRandomValue(field.getValueType(), randomStringSize))
                      .build());
            });

    return builder.build();
  }

  /**
   * Create a random Feast {@link Value} of {@link ValueType.Enum}.
   *
   * @param type {@link ValueType.Enum}
   * @param randomStringSize number of characters for the generated random string
   * @return {@link Value}
   */
  public static Value createRandomValue(ValueType.Enum type, int randomStringSize) {
    Value.Builder builder = Value.newBuilder();
    ThreadLocalRandom random = ThreadLocalRandom.current();

    switch (type) {
      case INVALID:
      case UNRECOGNIZED:
        throw new IllegalArgumentException("Invalid ValueType: " + type);
      case BYTES:
        builder.setBytesVal(
            ByteString.copyFrom(RandomStringUtils.randomAlphanumeric(randomStringSize).getBytes()));
        break;
      case STRING:
        builder.setStringVal(RandomStringUtils.randomAlphanumeric(randomStringSize));
        break;
      case INT32:
        builder.setInt32Val(random.nextInt());
        break;
      case INT64:
        builder.setInt64Val(random.nextLong());
        break;
      case DOUBLE:
        builder.setDoubleVal(random.nextDouble());
        break;
      case FLOAT:
        builder.setFloatVal(random.nextFloat());
        break;
      case BOOL:
        builder.setBoolVal(random.nextBoolean());
        break;
      case BYTES_LIST:
        builder.setBytesListVal(
            BytesList.newBuilder()
                .addVal(
                    ByteString.copyFrom(
                        RandomStringUtils.randomAlphanumeric(randomStringSize).getBytes()))
                .build());
        break;
      case STRING_LIST:
        builder.setStringListVal(
            StringList.newBuilder()
                .addVal(RandomStringUtils.randomAlphanumeric(randomStringSize))
                .build());
        break;
      case INT32_LIST:
        builder.setInt32ListVal(Int32List.newBuilder().addVal(random.nextInt()).build());
        break;
      case INT64_LIST:
        builder.setInt64ListVal(Int64List.newBuilder().addVal(random.nextLong()).build());
        break;
      case DOUBLE_LIST:
        builder.setDoubleListVal(DoubleList.newBuilder().addVal(random.nextDouble()).build());
        break;
      case FLOAT_LIST:
        builder.setFloatListVal(FloatList.newBuilder().addVal(random.nextFloat()).build());
        break;
      case BOOL_LIST:
        builder.setBoolListVal(BoolList.newBuilder().addVal(random.nextBoolean()).build());
        break;
    }
    return builder.build();
  }

  /**
   * Create {@link RedisKey} from {@link FeatureSet} and {@link FeatureRow}.
   *
   * <p>The entities in the created {@link RedisKey} will contain the value with matching field name
   * in the {@link FeatureRow}
   *
   * @param featureSetSpec {@link FeatureSetSpec}
   * @param row {@link FeatureSet}
   * @return {@link RedisKey}
   */
  public static RedisKey createRedisKey(FeatureSetSpec featureSetSpec, FeatureRow row) {
    RedisKey.Builder builder =
        RedisKey.newBuilder().setFeatureSet(getFeatureSetReference(featureSetSpec));
    featureSetSpec
        .getEntitiesList()
        .forEach(
            entityField ->
                row.getFieldsList().stream()
                    .filter(rowField -> rowField.getName().equals(entityField.getName()))
                    .findFirst()
                    .ifPresent(builder::addEntities));
    return builder.build();
  }

  private static class LocalZookeeper {

    static void start(int zookeeperPort, String zookeeperDataDir) {
      final ZooKeeperServerMain zookeeper = new ZooKeeperServerMain();
      final ServerConfig serverConfig = new ServerConfig();
      serverConfig.parse(new String[] {String.valueOf(zookeeperPort), zookeeperDataDir});
      new Thread(
              () -> {
                try {
                  zookeeper.runFromConfig(serverConfig);
                } catch (IOException e) {
                  e.printStackTrace();
                }
              })
          .start();
    }
  }

  // Modified version of
  // https://github.com/tim-group/java-statsd-client/blob/master/src/test/java/com/timgroup/statsd/NonBlockingStatsDClientTest.java
  @SuppressWarnings("CatchMayIgnoreException")
  public static class DummyStatsDServer {

    private final List<String> messagesReceived = new ArrayList<String>();
    private final DatagramSocket server;

    public DummyStatsDServer(int port) {
      try {
        server = new DatagramSocket(port);
      } catch (SocketException e) {
        throw new IllegalStateException(e);
      }
      new Thread(
              () -> {
                try {
                  while (true) {
                    final DatagramPacket packet = new DatagramPacket(new byte[65535], 65535);
                    server.receive(packet);
                    messagesReceived.add(
                        new String(packet.getData(), StandardCharsets.UTF_8).trim() + "\n");
                    // The sleep duration here is shorter than that used in waitForMessage() at
                    // 50ms.
                    // Otherwise sometimes some messages seem to be lost, leading to flaky tests.
                    Thread.sleep(15L);
                  }

                } catch (Exception e) {
                }
              })
          .start();
    }

    public void stop() {
      server.close();
    }

    public void waitForMessage() {
      while (messagesReceived.isEmpty()) {
        try {
          Thread.sleep(50L);
        } catch (InterruptedException e) {
        }
      }
    }

    public List<String> messagesReceived() {
      List<String> out = new ArrayList<>();
      for (String msg : messagesReceived) {
        String[] lines = msg.split("\n");
        out.addAll(Arrays.asList(lines));
      }
      return out;
    }
  }

  /**
   * Create a field object with given name and type.
   *
   * @param name of the field.
   * @param value of the field. Should be compatible with the valuetype given.
   * @param valueType type of the field.
   * @return Field object
   */
  public static Field field(String name, Object value, ValueType.Enum valueType) {
    Field.Builder fieldBuilder = Field.newBuilder().setName(name);
    switch (valueType) {
      case INT32:
        return fieldBuilder.setValue(Value.newBuilder().setInt32Val((int) value)).build();
      case INT64:
        return fieldBuilder.setValue(Value.newBuilder().setInt64Val((int) value)).build();
      case FLOAT:
        return fieldBuilder.setValue(Value.newBuilder().setFloatVal((float) value)).build();
      case DOUBLE:
        return fieldBuilder.setValue(Value.newBuilder().setDoubleVal((double) value)).build();
      case STRING:
        return fieldBuilder.setValue(Value.newBuilder().setStringVal((String) value)).build();
      default:
        throw new IllegalStateException("Unexpected valueType: " + value.getClass());
    }
  }

  /**
   * This blocking method waits until an ImportJob pipeline has written all elements to the store.
   *
   * <p>The pipeline must be in the RUNNING state before calling this method.
   *
   * @param pipelineResult result of running the Pipeline
   * @param maxWaitDuration wait until this max amount of duration
   * @throws InterruptedException if the thread is interruped while waiting
   */
  public static void waitUntilAllElementsAreWrittenToStore(
      PipelineResult pipelineResult, Duration maxWaitDuration, Duration checkInterval)
      throws InterruptedException {
    if (pipelineResult.getState().isTerminal()) {
      return;
    }

    if (!pipelineResult.getState().equals(State.RUNNING)) {
      throw new IllegalArgumentException(
          "Pipeline must be in RUNNING state before calling this method.");
    }

    MetricResults metricResults;
    try {
      metricResults = pipelineResult.metrics();
    } catch (UnsupportedOperationException e) {
      // Runner does not support metrics so we just wait as long as we are allowed to.
      Thread.sleep(maxWaitDuration.getMillis());
      return;
    }

    String writeToStoreMetric =
        WriteSuccessMetricsTransform.METRIC_NAMESPACE
            + ":"
            + WriteSuccessMetricsTransform.ELEMENTS_WRITTEN_METRIC;
    long committed = 0;
    long maxSystemTimeMillis = System.currentTimeMillis() + maxWaitDuration.getMillis();

    while (System.currentTimeMillis() <= maxSystemTimeMillis) {
      Thread.sleep(checkInterval.getMillis());

      for (MetricResult<Long> metricResult : metricResults.allMetrics().getCounters()) {
        // We are only concerned with the metric: count of elements that have been
        // written to the store.
        if (!metricResult.getName().toString().contains(writeToStoreMetric)) {
          continue;
        }
        try {
          // If between check interval, no more changes in the no of committed elements
          // we can assume the pipeline has finished writing all the elements to store.
          if (metricResult.getCommitted() == committed) {
            return;
          }
          committed = metricResult.getCommitted();
          break;
        } catch (UnsupportedOperationException e) {
          // Runner does not support committed metrics so we just wait as long as we are allowed to.
          Thread.sleep(maxWaitDuration.getMillis());
          return;
        }
      }
    }
  }
}
