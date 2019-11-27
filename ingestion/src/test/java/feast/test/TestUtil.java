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

import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.storage.RedisProto.RedisKey;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FeatureRowProto.FeatureRow.Builder;
import feast.types.FieldProto.Field;
import feast.types.ValueProto.BoolList;
import feast.types.ValueProto.BytesList;
import feast.types.ValueProto.DoubleList;
import feast.types.ValueProto.FloatList;
import feast.types.ValueProto.Int32List;
import feast.types.ValueProto.Int64List;
import feast.types.ValueProto.StringList;
import feast.types.ValueProto.Value;
import feast.types.ValueProto.ValueType;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
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
   */
  public static FeatureRow createRandomFeatureRow(FeatureSetSpec spec) {
    ThreadLocalRandom random = ThreadLocalRandom.current();
    int randomStringSizeMaxSize = 12;
    return createRandomFeatureRow(spec, random.nextInt(0, randomStringSizeMaxSize) + 4);
  }

  /**
   * Create a Feature Row with random value according to the FeatureSetSpec.
   *
   * <p>The Feature Row created contains fields according to the entities and features defined in
   * FeatureSetSpec, matching the value type of the field, with randomized value for testing.
   *
   * @param spec {@link FeatureSetSpec}
   * @param randomStringSize number of characters for the generated random string
   * @return {@link FeatureRow}
   */
  public static FeatureRow createRandomFeatureRow(FeatureSetSpec spec, int randomStringSize) {
    Builder builder =
        FeatureRow.newBuilder()
            .setFeatureSet(spec.getName() + ":" + spec.getVersion())
            .setEventTimestamp(Timestamps.fromMillis(System.currentTimeMillis()));

    spec.getEntitiesList()
        .forEach(
            field -> {
              builder.addFields(
                  Field.newBuilder()
                      .setName(field.getName())
                      .setValue(createRandomValue(field.getValueType(), randomStringSize))
                      .build());
            });

    spec.getFeaturesList()
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
   * Create {@link RedisKey} from {@link FeatureSetSpec} and {@link FeatureRow}.
   *
   * <p>The entities in the created {@link RedisKey} will contain the value with matching field name
   * in the {@link FeatureRow}
   *
   * @param spec {@link FeatureSetSpec}
   * @param row {@link FeatureSetSpec}
   * @return {@link RedisKey}
   */
  public static RedisKey createRedisKey(FeatureSetSpec spec, FeatureRow row) {
    RedisKey.Builder builder =
        RedisKey.newBuilder().setFeatureSet(spec.getName() + ":" + spec.getVersion());
    spec.getEntitiesList()
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
}
