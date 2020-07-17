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
package feast.core.service;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.beans.HasPropertyWithValue.hasProperty;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.core.AllOf.allOf;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.dao.JobRepository;
import feast.core.it.BaseIT;
import feast.core.it.DataGenerator;
import feast.core.it.SimpleAPIClient;
import feast.core.job.JobManager;
import feast.core.job.Runner;
import feast.core.model.*;
import feast.proto.core.*;
import feast.proto.types.ValueProto;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.*;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootTest(
    properties = {
      "feast.jobs.enabled=true",
      "feast.jobs.polling_interval_milliseconds=2000",
      "feast.stream.specsOptions.notifyIntervalMilliseconds=100",
      "feast.jobs.consolidate-jobs-per-source=true"
    })
public class JobCoordinatorIT extends BaseIT {
  @Autowired private FakeJobManager jobManager;

  @Autowired private JobRepository jobRepository;

  @Autowired KafkaTemplate<String, IngestionJobProto.FeatureSetSpecAck> ackPublisher;

  static CoreServiceGrpc.CoreServiceBlockingStub stub;
  static List<FeatureSetProto.FeatureSetSpec> specsMailbox = new ArrayList<>();
  static SimpleAPIClient apiClient;

  @BeforeAll
  public static void globalSetUp(@Value("${grpc.server.port}") int port) {
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
    stub = CoreServiceGrpc.newBlockingStub(channel);
    apiClient = new SimpleAPIClient(stub);
  }

  @BeforeEach
  public void setUp(TestInfo testInfo) {
    apiClient.updateStore(DataGenerator.getDefaultStore());

    specsMailbox = new ArrayList<>();

    if (!isNestedTest(testInfo)) {
      jobManager.cleanAll();
    }
  }

  @KafkaListener(
      topics = {"${feast.stream.specsOptions.specsTopic}"},
      containerFactory = "testListenerContainerFactory")
  public void listenSpecs(ConsumerRecord<String, byte[]> record)
      throws InvalidProtocolBufferException {
    FeatureSetProto.FeatureSetSpec featureSetSpec =
        FeatureSetProto.FeatureSetSpec.parseFrom(record.value());
    specsMailbox.add(featureSetSpec);
  }

  @Test
  public void shouldCreateJobForNewSource() {
    apiClient.simpleApplyFeatureSet(
        DataGenerator.getDefaultSource(),
        "default",
        "test",
        Collections.emptyList(),
        Collections.emptyList());

    List<FeatureSetProto.FeatureSet> featureSets = apiClient.simpleListFeatureSets("*");
    assertThat(featureSets.size(), equalTo(1));

    await()
        .until(
            jobManager::getAllJobs,
            hasItem(
                allOf(
                    hasProperty("runner", equalTo(Runner.DIRECT)),
                    hasProperty("id", containsString("kafka--627317556")),
                    hasProperty("jobStores", hasSize(1)),
                    hasProperty("featureSetJobStatuses", hasSize(1)))));
  }

  @Test
  public void shouldUpgradeJobWhenStoreChanged() {
    apiClient.simpleApplyFeatureSet(
        DataGenerator.getDefaultSource(),
        "project",
        "test",
        Collections.emptyList(),
        Collections.emptyList());

    await().until(jobManager::getAllJobs, hasSize(1));

    apiClient.updateStore(
        DataGenerator.createStore(
            "new-store",
            StoreProto.Store.StoreType.REDIS,
            ImmutableList.of(DataGenerator.getDefaultSubscription())));

    await()
        .until(
            jobManager::getAllJobs,
            containsInAnyOrder(
                allOf(
                    hasProperty("jobStores", hasSize(2)),
                    hasProperty("featureSetJobStatuses", hasSize(1)))));

    await().until(jobManager::getAllJobs, hasSize(1));
  }

  @Test
  public void shouldRestoreJobThatStopped() {
    apiClient.simpleApplyFeatureSet(
        DataGenerator.getDefaultSource(),
        "project",
        "test",
        Collections.emptyList(),
        Collections.emptyList());

    await().until(jobManager::getAllJobs, hasSize(1));
    Job job = jobRepository.findByStatus(JobStatus.RUNNING).get(0);

    List<IngestionJobProto.IngestionJob> ingestionJobs = apiClient.listIngestionJobs();
    assertThat(ingestionJobs, hasSize(1));
    assertThat(ingestionJobs, containsInAnyOrder(hasProperty("id", equalTo(job.getId()))));

    apiClient.restartIngestionJob(ingestionJobs.get(0).getId());

    await().until(() -> jobManager.getJobStatus(job), equalTo(JobStatus.ABORTED));

    await()
        .until(
            apiClient::listIngestionJobs,
            hasItem(
                allOf(
                    hasProperty("status", equalTo(IngestionJobProto.IngestionJobStatus.RUNNING)),
                    hasProperty("id", not(ingestionJobs.get(0).getId())))));
  }

  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  @Nested
  class SpecNotificationFlow extends SequentialFlow {
    Job job;

    @Test
    @Order(1)
    public void shouldSendNewSpec() {
      jobManager.cleanAll();

      job =
          Job.builder()
              .setSource(Source.fromProto(DataGenerator.getDefaultSource()))
              .setId("some-running-id")
              .setStatus(JobStatus.RUNNING)
              .build();

      jobManager.startJob(job);

      job.setStores(ImmutableSet.of(Store.fromProto(DataGenerator.getDefaultStore())));
      jobRepository.saveAndFlush(job);

      apiClient.simpleApplyFeatureSet(
          DataGenerator.getDefaultSource(),
          "default",
          "test",
          ImmutableList.of(Pair.of("entity", ValueProto.ValueType.Enum.BOOL)),
          Collections.emptyList());

      FeatureSetProto.FeatureSet featureSet = apiClient.simpleGetFeatureSet("default", "test");

      assertThat(
          featureSet.getMeta().getStatus(),
          equalTo(FeatureSetProto.FeatureSetStatus.STATUS_PENDING));

      await().until(() -> specsMailbox, hasSize(1));

      assertThat(
          specsMailbox.get(0),
          allOf(
              hasProperty("project", equalTo("default")),
              hasProperty("name", equalTo("test")),
              hasProperty("entitiesList", hasSize(1)),
              hasProperty("version", equalTo(1))));

      assertThat(jobRepository.findByStatus(JobStatus.RUNNING), hasSize(1));
    }

    @Test
    @Order(2)
    public void shouldUpdateSpec() {
      apiClient.simpleApplyFeatureSet(
          DataGenerator.getDefaultSource(),
          "default",
          "test",
          ImmutableList.of(Pair.of("entity", ValueProto.ValueType.Enum.BOOL)),
          ImmutableList.of(Pair.of("feature", ValueProto.ValueType.Enum.INT32)));

      await().until(() -> specsMailbox, hasSize(1));

      assertThat(
          specsMailbox.get(0),
          allOf(
              hasProperty("project", equalTo("default")),
              hasProperty("name", equalTo("test")),
              hasProperty("featuresList", hasSize(1)),
              hasProperty("version", equalTo(2))));
    }

    @Test
    @Order(3)
    public void shouldIgnoreOutdatedACKs() throws InterruptedException {
      ackPublisher.sendDefault(
          "default/test",
          IngestionJobProto.FeatureSetSpecAck.newBuilder()
              .setFeatureSetVersion(1)
              .setJobName(job.getId())
              .setFeatureSetReference("default/test")
              .build());

      // time to process
      Thread.sleep(1000);

      FeatureSetProto.FeatureSet featureSet = apiClient.simpleGetFeatureSet("default", "test");

      assertThat(
          featureSet.getMeta().getStatus(),
          equalTo(FeatureSetProto.FeatureSetStatus.STATUS_PENDING));
    }

    @Test
    @Order(4)
    public void shouldUpdateDeliveryStatus() {
      ackPublisher.sendDefault(
          "default/test",
          IngestionJobProto.FeatureSetSpecAck.newBuilder()
              .setFeatureSetVersion(2)
              .setJobName(job.getId())
              .setFeatureSetReference("default/test")
              .build());

      await()
          .until(
              () -> apiClient.simpleGetFeatureSet("default", "test").getMeta().getStatus(),
              equalTo(FeatureSetProto.FeatureSetStatus.STATUS_READY));
    }

    @Test
    @Order(5)
    public void shouldReallocateFeatureSetAfterSourceChanged() {
      assertThat(jobManager.getJobStatus(job), equalTo(JobStatus.RUNNING));

      apiClient.simpleApplyFeatureSet(
          DataGenerator.createSource("localhost", "newTopic"),
          "default",
          "test",
          ImmutableList.of(Pair.of("entity", ValueProto.ValueType.Enum.BOOL)),
          ImmutableList.of(Pair.of("feature", ValueProto.ValueType.Enum.INT32)));

      await().until(() -> jobManager.getJobStatus(job), equalTo(JobStatus.ABORTED));

      await().until(() -> jobRepository.findByStatus(JobStatus.RUNNING), hasSize(1));

      assertThat(
          specsMailbox.get(0),
          allOf(
              hasProperty("project", equalTo("default")),
              hasProperty("name", equalTo("test")),
              hasProperty("version", equalTo(3))));
    }

    @Test
    @Order(6)
    public void shouldUpdateStatusAfterACKfromNewJob() {
      job = jobRepository.findByStatus(JobStatus.RUNNING).get(0);

      ackPublisher.sendDefault(
          "default/test",
          IngestionJobProto.FeatureSetSpecAck.newBuilder()
              .setFeatureSetVersion(3)
              .setJobName(job.getId())
              .setFeatureSetReference("default/test")
              .build());

      await()
          .until(
              () -> apiClient.simpleGetFeatureSet("default", "test").getMeta().getStatus(),
              equalTo(FeatureSetProto.FeatureSetStatus.STATUS_READY));
    }
  }

  public static class FakeJobManager implements JobManager {
    private final Map<String, Job> state;

    public FakeJobManager() {
      state = new HashMap<>();
    }

    @Override
    public Runner getRunnerType() {
      return Runner.DIRECT;
    }

    @Override
    public Job startJob(Job job) {
      String extId = UUID.randomUUID().toString();
      job.setExtId(extId);
      job.setStatus(JobStatus.RUNNING);
      state.put(extId, job);
      return job;
    }

    @Override
    public Job updateJob(Job job) {
      return job;
    }

    @Override
    public Job abortJob(Job job) {
      job.setStatus(JobStatus.ABORTING);
      state.remove(job.getExtId());
      return job;
    }

    @Override
    public Job restartJob(Job job) {
      return abortJob(job);
    }

    @Override
    public JobStatus getJobStatus(Job job) {
      if (state.containsKey(job.getExtId())) {
        return JobStatus.RUNNING;
      }

      return JobStatus.ABORTED;
    }

    public List<Job> getAllJobs() {
      return Lists.newArrayList(state.values());
    }

    public void cleanAll() {
      state.clear();
    }
  }

  @TestConfiguration
  public static class TestConfig extends BaseTestConfig {
    @Bean
    public JobManager getJobManager() {
      return new FakeJobManager();
    }
  }
}
