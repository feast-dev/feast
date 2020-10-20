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

import static feast.common.models.FeatureSet.getFeatureSetStringRef;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import feast.common.models.FeatureSetReference;
import feast.proto.core.FeatureSetProto.FeatureSet;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.storage.RedisProto.RedisKey;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FeatureRowProto.FeatureRow.Builder;
import feast.proto.types.FieldProto.Field;
import feast.proto.types.ValueProto.BoolList;
import feast.proto.types.ValueProto.BytesList;
import feast.proto.types.ValueProto.DoubleList;
import feast.proto.types.ValueProto.FloatList;
import feast.proto.types.ValueProto.Int32List;
import feast.proto.types.ValueProto.Int64List;
import feast.proto.types.ValueProto.StringList;
import feast.proto.types.ValueProto.Value;
import feast.proto.types.ValueProto.ValueType;
import java.io.BufferedReader;
import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;

@SuppressWarnings("WeakerAccess")
public class TestUtil {

  /**
   * Publish test Feature Row messages to a running Kafka broker
   *
   * @param bootstrapServers e.g. localhost:9092
   * @param topic e.g. my_topic
   * @param messages e.g. list of Feature Row
   * @param valueSerializer in Feast this valueSerializer should be "ByteArraySerializer.class"
   * @param publishTimeoutSec duration to wait for publish operation (of each message) to succeed
   */
  public static <T extends Message> void publishToKafka(
      String bootstrapServers,
      String topic,
      List<Pair<String, T>> messages,
      Class<?> valueSerializer,
      long publishTimeoutSec) {

    Properties prop = new Properties();
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
    Producer<String, byte[]> producer = new KafkaProducer<>(prop);

    messages.forEach(
        featureRow -> {
          ProducerRecord<String, byte[]> record =
              new ProducerRecord<>(
                  topic, featureRow.getLeft(), featureRow.getRight().toByteArray());
          try {
            producer.send(record).get(publishTimeoutSec, TimeUnit.SECONDS);
          } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
          }
        });
  }

  public static <T> KafkaConsumer<String, T> makeKafkaConsumer(
      String bootstrapServers, String topic, Class<?> valueDeserializer) {
    Properties prop = new Properties();
    prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
    prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    prop.put(ConsumerConfig.GROUP_ID_CONFIG, "test");

    KafkaConsumer<String, T> consumer = new KafkaConsumer<>(prop);

    consumer.subscribe(ImmutableList.of(topic));
    return consumer;
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
            .setFeatureSet(getFeatureSetStringRef(featureSetSpec))
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
        RedisKey.newBuilder().setFeatureSet(getFeatureSetStringRef(featureSetSpec));
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

    public static Thread thread;

    static void start(int zookeeperPort, String zookeeperDataDir) {
      ZooKeeperServerMain zookeeper = new ZooKeeperServerMain();
      final ServerConfig serverConfig = new ServerConfig();
      serverConfig.parse(new String[] {String.valueOf(zookeeperPort), zookeeperDataDir});
      thread =
          new Thread(
              () -> {
                try {
                  zookeeper.runFromConfig(serverConfig);
                } catch (Exception e) {
                  e.printStackTrace();
                }
              });
      thread.start();
    }

    static void stop() {
      thread.interrupt();
    }
  }

  @SuppressWarnings("CatchMayIgnoreException")
  public static class DummyStatsDServer {

    private final CopyOnWriteArrayList<String> messagesReceived = new CopyOnWriteArrayList<>();
    private final DatagramSocket server;

    public DummyStatsDServer(int port) {
      try {
        server = new DatagramSocket(port);
        server.setReceiveBufferSize(131072);
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
                  }
                } catch (Exception e) {
                }
              })
          .start();
    }

    public void stop() {
      server.close();
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

  public static void statsDMessagesCompare(List<String> expectedLines, List<String> actualLines) {
    for (String expected : expectedLines) {
      boolean matched = false;
      for (String actual : actualLines) {

        if (actual.equals(expected)) {
          matched = true;
          break;
        }
      }
      if (!matched) {
        System.out.println("Print actual metrics output for debugging:");
        for (String line : actualLines) {
          System.out.println(line);
        }
        fail(String.format("Expected StatsD metric not found:\n%s", expected));
      }
    }
  }

  public static Map<FeatureSetReference, Iterable<FeatureRow>> readTestInput(String path)
      throws IOException {
    return readTestInput(path, null);
  }

  // Test utility method to create test feature row data from a text file.
  // If tsOverride is not null, all the feature row will have the same timestamp "tsOverride".
  // Else if there exist a "timestamp" column with RFC3339 format, the feature row will be assigned
  // that timestamp.
  // Else no timestamp will be assigned (the feature row will have the default proto Timestamp
  // object).
  @SuppressWarnings("SameParameterValue")
  public static Map<FeatureSetReference, Iterable<FeatureRow>> readTestInput(
      String path, Timestamp tsOverride) throws IOException {
    Map<FeatureSetReference, List<FeatureRow>> data = new HashMap<>();
    URL url = Thread.currentThread().getContextClassLoader().getResource(path);
    if (url == null) {
      throw new IllegalArgumentException(
          "cannot read test data, path contains null url. Path: " + path);
    }
    List<String> lines = new ArrayList<>();
    try (BufferedReader reader = java.nio.file.Files.newBufferedReader(Paths.get(url.getPath()))) {
      String line = reader.readLine();
      while (line != null) {
        lines.add(line);
        line = reader.readLine();
      }
    }
    List<String> colNames = new ArrayList<>();
    for (String line : lines) {
      if (line.trim().length() < 1) {
        continue;
      }
      String[] splits = line.split(",");
      colNames.addAll(Arrays.asList(splits));

      if (line.startsWith("featuresetref")) {
        // Header line
        colNames.addAll(Arrays.asList(splits).subList(1, splits.length));
        continue;
      }

      Builder featureRowBuilder = FeatureRow.newBuilder();
      for (int i = 0; i < splits.length; i++) {
        String colVal = splits[i].trim();
        if (i == 0) {
          featureRowBuilder.setFeatureSet(colVal);
          continue;
        }
        String colName = colNames.get(i);
        if (colName.equals("timestamp")) {
          Instant instant = Instant.parse(colVal);
          featureRowBuilder.setEventTimestamp(
              Timestamps.fromNanos(instant.getEpochSecond() * 1_000_000_000 + instant.getNano()));
          continue;
        }

        Field.Builder fieldBuilder = Field.newBuilder().setName(colName);
        if (!colVal.isEmpty()) {
          switch (colName) {
            case "int32":
              fieldBuilder.setValue(Value.newBuilder().setInt32Val((Integer.parseInt(colVal))));
              break;
            case "int64":
              fieldBuilder.setValue(Value.newBuilder().setInt64Val((Long.parseLong(colVal))));
              break;
            case "double":
              fieldBuilder.setValue(Value.newBuilder().setDoubleVal((Double.parseDouble(colVal))));
              break;
            case "float":
              fieldBuilder.setValue(Value.newBuilder().setFloatVal((Float.parseFloat(colVal))));
              break;
            case "bool":
              fieldBuilder.setValue(Value.newBuilder().setBoolVal((Boolean.parseBoolean(colVal))));
              break;
            case "int32list":
              List<Integer> int32List = new ArrayList<>();
              for (String val : colVal.split("\\|")) {
                int32List.add(Integer.parseInt(val));
              }
              fieldBuilder.setValue(
                  Value.newBuilder().setInt32ListVal(Int32List.newBuilder().addAllVal(int32List)));
              break;
            case "int64list":
              List<Long> int64list = new ArrayList<>();
              for (String val : colVal.split("\\|")) {
                int64list.add(Long.parseLong(val));
              }
              fieldBuilder.setValue(
                  Value.newBuilder().setInt64ListVal(Int64List.newBuilder().addAllVal(int64list)));
              break;
            case "doublelist":
              List<Double> doubleList = new ArrayList<>();
              for (String val : colVal.split("\\|")) {
                doubleList.add(Double.parseDouble(val));
              }
              fieldBuilder.setValue(
                  Value.newBuilder()
                      .setDoubleListVal(DoubleList.newBuilder().addAllVal(doubleList)));
              break;
            case "floatlist":
              List<Float> floatList = new ArrayList<>();
              for (String val : colVal.split("\\|")) {
                floatList.add(Float.parseFloat(val));
              }
              fieldBuilder.setValue(
                  Value.newBuilder().setFloatListVal(FloatList.newBuilder().addAllVal(floatList)));
              break;
            case "boollist":
              List<Boolean> boolList = new ArrayList<>();
              for (String val : colVal.split("\\|")) {
                boolList.add(Boolean.parseBoolean(val));
              }
              fieldBuilder.setValue(
                  Value.newBuilder().setBoolListVal(BoolList.newBuilder().addAllVal(boolList)));
              break;
            case "bytes":
              fieldBuilder.setValue(
                  Value.newBuilder().setBytesVal(ByteString.copyFromUtf8("Dummy")));
              break;
            case "byteslist":
              fieldBuilder.setValue(
                  Value.newBuilder().setBytesListVal(BytesList.getDefaultInstance()));
              break;
            case "string":
              fieldBuilder.setValue(Value.newBuilder().setStringVal("Dummy"));
              break;
            case "stringlist":
              fieldBuilder.setValue(
                  Value.newBuilder().setStringListVal(StringList.getDefaultInstance()));
              break;
          }
        }
        featureRowBuilder.addFields(fieldBuilder);
      }

      FeatureSetReference featureSetRef =
          FeatureSetReference.parse(featureRowBuilder.getFeatureSet());
      if (!data.containsKey(featureSetRef)) {
        data.put(featureSetRef, new ArrayList<>());
      }
      List<FeatureRow> featureRowsByFeatureSetRef = data.get(featureSetRef);
      if (tsOverride != null) {
        featureRowBuilder.setEventTimestamp(tsOverride);
      }
      featureRowsByFeatureSetRef.add(featureRowBuilder.build());
    }

    // Convert List<FeatureRow> to Iterable<FeatureRow> to match the function signature in
    // WriteFeatureValueMetricsDoFn
    Map<FeatureSetReference, Iterable<FeatureRow>> dataWithIterable = new HashMap<>();
    for (Map.Entry<FeatureSetReference, List<FeatureRow>> entrySet : data.entrySet()) {
      FeatureSetReference key = entrySet.getKey();
      Iterable<FeatureRow> value = entrySet.getValue();
      dataWithIterable.put(key, value);
    }
    return dataWithIterable;
  }

  // Test utility method to read expected StatsD metrics output from a text file.
  @SuppressWarnings("SameParameterValue")
  public static List<String> readTestOutput(String path) throws IOException {
    URL url = Thread.currentThread().getContextClassLoader().getResource(path);
    if (url == null) {
      throw new IllegalArgumentException(
          "cannot read test data, path contains null url. Path: " + path);
    }
    List<String> lines = new ArrayList<>();
    try (BufferedReader reader = java.nio.file.Files.newBufferedReader(Paths.get(url.getPath()))) {
      String line = reader.readLine();
      while (line != null) {
        if (line.trim().length() > 1) {
          lines.add(line);
        }
        line = reader.readLine();
      }
    }
    return lines;
  }
}
