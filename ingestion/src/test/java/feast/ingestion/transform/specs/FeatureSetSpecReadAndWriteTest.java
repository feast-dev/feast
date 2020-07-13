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
package feast.ingestion.transform.specs;

import static feast.common.models.FeatureSet.getFeatureSetStringRef;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.IngestionJobProto;
import feast.proto.core.SourceProto;
import feast.proto.core.StoreProto;
import feast.test.TestUtil;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.*;

public class FeatureSetSpecReadAndWriteTest {
  @Rule public transient TestPipeline p = TestPipeline.fromOptions(makePipelineOptions());

  private static final String KAFKA_HOST = "localhost";
  private static final int KAFKA_PORT = 29092;
  private static final String KAFKA_BOOTSTRAP_SERVERS = KAFKA_HOST + ":" + KAFKA_PORT;
  private static final short KAFKA_REPLICATION_FACTOR = 1;
  private static final String KAFKA_TOPIC = "topic";
  private static final String KAFKA_SPECS_TOPIC = "topic_specs";
  private static final String KAFKA_SPECS_ACK_TOPIC = "topic_specs_ack";

  private static final long KAFKA_PUBLISH_TIMEOUT_SEC = 10;
  private static final long KAFKA_POLL_TIMEOUT_SEC = 10;

  private KafkaConsumer<String, IngestionJobProto.FeatureSetSpecAck> consumer;

  @SuppressWarnings("UnstableApiUsage")
  private static final String ZOOKEEPER_DATA_DIR = Files.createTempDir().getAbsolutePath();

  private static final String ZOOKEEPER_HOST = "localhost";
  private static final int ZOOKEEPER_PORT = 2183;

  @BeforeClass
  public static void setupClass() throws IOException, InterruptedException {
    TestUtil.LocalKafka.start(
        KAFKA_HOST,
        KAFKA_PORT,
        KAFKA_REPLICATION_FACTOR,
        true,
        ZOOKEEPER_HOST,
        ZOOKEEPER_PORT,
        ZOOKEEPER_DATA_DIR);
  }

  @Before
  public void setup() {
    consumer =
        TestUtil.makeKafkaConsumer(
            KAFKA_BOOTSTRAP_SERVERS, KAFKA_SPECS_ACK_TOPIC, AckMessageDeserializer.class);
  }

  @AfterClass
  public static void tearDown() {
    TestUtil.LocalKafka.stop();
  }

  public static PipelineOptions makePipelineOptions() {
    DirectOptions options = PipelineOptionsFactory.as(DirectOptions.class);
    options.setJobName("test_job");
    options.setBlockOnRun(false);
    return options;
  }

  @Test
  public void pipelineShouldReadSpecsAndAcknowledge() throws InterruptedException {
    SourceProto.Source source =
        SourceProto.Source.newBuilder()
            .setKafkaSourceConfig(
                SourceProto.KafkaSourceConfig.newBuilder()
                    .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                    .setTopic(KAFKA_TOPIC)
                    .build())
            .build();

    StoreProto.Store store =
        StoreProto.Store.newBuilder()
            .addSubscriptions(
                StoreProto.Store.Subscription.newBuilder()
                    .setProject("project")
                    .setName("*")
                    .build())
            .build();

    IngestionJobProto.SpecsStreamingUpdateConfig specsStreamingUpdateConfig =
        IngestionJobProto.SpecsStreamingUpdateConfig.newBuilder()
            .setSource(
                SourceProto.KafkaSourceConfig.newBuilder()
                    .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                    .setTopic(KAFKA_SPECS_TOPIC)
                    .build())
            .setAck(
                SourceProto.KafkaSourceConfig.newBuilder()
                    .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                    .setTopic(KAFKA_SPECS_ACK_TOPIC)
                    .build())
            .build();

    p.apply(
            ReadFeatureSetSpecs.newBuilder()
                .setSource(source)
                .setStores(ImmutableList.of(store))
                .setSpecsStreamingUpdateConfig(specsStreamingUpdateConfig)
                .build())
        .apply(Keys.create())
        .apply(
            WriteFeatureSetSpecAck.newBuilder()
                .setSinksCount(1)
                .setSpecsStreamingUpdateConfig(specsStreamingUpdateConfig)
                .build());

    // specs' history is being compacted on the initial read
    publishSpecToKafka("project", "fs", 1, source);
    publishSpecToKafka("project", "fs", 2, source);
    publishSpecToKafka("project", "fs", 3, source);
    publishSpecToKafka("project", "fs_2", 2, source);

    p.run();
    Thread.sleep(10000);

    List<IngestionJobProto.FeatureSetSpecAck> acks = getFeatureSetSpecAcks();

    assertThat(
        acks,
        hasItem(
            IngestionJobProto.FeatureSetSpecAck.newBuilder()
                .setJobName("test_job")
                .setFeatureSetVersion(3)
                .setFeatureSetReference("project/fs")
                .build()));
    assertThat(
        acks,
        hasItem(
            IngestionJobProto.FeatureSetSpecAck.newBuilder()
                .setJobName("test_job")
                .setFeatureSetVersion(2)
                .setFeatureSetReference("project/fs_2")
                .build()));

    // in-flight update 1
    publishSpecToKafka("project", "fs", 4, source);

    Thread.sleep(5000);

    assertThat(
        getFeatureSetSpecAcks(),
        hasItem(
            IngestionJobProto.FeatureSetSpecAck.newBuilder()
                .setJobName("test_job")
                .setFeatureSetVersion(4)
                .setFeatureSetReference("project/fs")
                .build()));

    // in-flight update 2
    publishSpecToKafka("project", "fs_2", 3, source);

    Thread.sleep(5000);

    assertThat(
        getFeatureSetSpecAcks(),
        hasItem(
            IngestionJobProto.FeatureSetSpecAck.newBuilder()
                .setJobName("test_job")
                .setFeatureSetVersion(3)
                .setFeatureSetReference("project/fs_2")
                .build()));
  }

  private List<IngestionJobProto.FeatureSetSpecAck> getFeatureSetSpecAcks() {
    ConsumerRecords<String, IngestionJobProto.FeatureSetSpecAck> consumerRecords =
        consumer.poll(java.time.Duration.ofSeconds(KAFKA_POLL_TIMEOUT_SEC));

    return Lists.newArrayList(consumerRecords.records(KAFKA_SPECS_ACK_TOPIC)).stream()
        .map(ConsumerRecord::value)
        .collect(Collectors.toList());
  }

  private void publishSpecToKafka(
      String project, String name, int version, SourceProto.Source source) {
    FeatureSetProto.FeatureSetSpec spec =
        FeatureSetProto.FeatureSetSpec.newBuilder()
            .setProject(project)
            .setName(name)
            .setVersion(version)
            .setSource(source)
            .build();

    TestUtil.publishToKafka(
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_SPECS_TOPIC,
        ImmutableList.of(Pair.of(getFeatureSetStringRef(spec), spec)),
        ByteArraySerializer.class,
        KAFKA_PUBLISH_TIMEOUT_SEC);
  }

  public static class AckMessageDeserializer
      implements Deserializer<IngestionJobProto.FeatureSetSpecAck> {

    @Override
    public IngestionJobProto.FeatureSetSpecAck deserialize(String topic, byte[] data) {
      try {
        return IngestionJobProto.FeatureSetSpecAck.parseFrom(data);
      } catch (InvalidProtocolBufferException e) {
        e.printStackTrace();
        return null;
      }
    }
  }
}
