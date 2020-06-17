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
package feast.core.service;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.config.FeastProperties;
import feast.core.config.FeastProperties.JobProperties;
import feast.core.dao.FeatureSetRepository;
import feast.core.dao.JobRepository;
import feast.core.job.JobManager;
import feast.core.job.JobMatcher;
import feast.core.job.Runner;
import feast.core.model.*;
import feast.core.util.ModelHelpers;
import feast.proto.core.CoreServiceProto.ListFeatureSetsRequest.Filter;
import feast.proto.core.CoreServiceProto.ListFeatureSetsResponse;
import feast.proto.core.CoreServiceProto.ListStoresResponse;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.FeatureSetProto.FeatureSetMeta;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.IngestionJobProto;
import feast.proto.core.SourceProto.KafkaSourceConfig;
import feast.proto.core.SourceProto.Source;
import feast.proto.core.SourceProto.SourceType;
import feast.proto.core.StoreProto;
import feast.proto.core.StoreProto.Store.RedisConfig;
import feast.proto.core.StoreProto.Store.StoreType;
import feast.proto.core.StoreProto.Store.Subscription;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.AsyncResult;

public class JobCoordinatorServiceTest {

  @Rule public final ExpectedException exception = ExpectedException.none();
  @Mock JobRepository jobRepository;
  @Mock JobManager jobManager;
  @Mock SpecService specService;
  @Mock FeatureSetRepository featureSetRepository;
  @Mock private KafkaTemplate<String, FeatureSetSpec> kafkaTemplate;

  private FeastProperties feastProperties;
  private JobCoordinatorService jcs;

  @Before
  public void setUp() {
    initMocks(this);
    feastProperties = new FeastProperties();
    JobProperties jobProperties = new JobProperties();
    jobProperties.setJobUpdateTimeoutSeconds(5);
    feastProperties.setJobs(jobProperties);

    jcs =
        new JobCoordinatorService(
            jobRepository,
            featureSetRepository,
            specService,
            jobManager,
            feastProperties,
            kafkaTemplate);

    when(kafkaTemplate.sendDefault(any(), any())).thenReturn(new AsyncResult<>(null));
  }

  @Test
  public void shouldDoNothingIfNoStoresFound() throws InvalidProtocolBufferException {
    when(specService.listStores(any())).thenReturn(ListStoresResponse.newBuilder().build());

    jcs.Poll();
    verify(jobRepository, times(0)).saveAndFlush(any());
  }

  @Test
  public void shouldDoNothingIfNoMatchingFeatureSetsFound() throws InvalidProtocolBufferException {
    StoreProto.Store store =
        StoreProto.Store.newBuilder()
            .setName("test")
            .setType(StoreType.REDIS)
            .setRedisConfig(RedisConfig.newBuilder().build())
            .addSubscriptions(Subscription.newBuilder().setProject("*").setName("*").build())
            .build();
    when(specService.listStores(any()))
        .thenReturn(ListStoresResponse.newBuilder().addStore(store).build());
    when(specService.listFeatureSets(
            Filter.newBuilder().setProject("*").setFeatureSetName("*").build()))
        .thenReturn(ListFeatureSetsResponse.newBuilder().build());

    jcs.Poll();
    verify(jobRepository, times(0)).saveAndFlush(any());
  }

  @Test
  public void shouldGenerateAndSubmitJobsIfAny() throws InvalidProtocolBufferException {
    StoreProto.Store store =
        StoreProto.Store.newBuilder()
            .setName("test")
            .setType(StoreType.REDIS)
            .setRedisConfig(RedisConfig.newBuilder().build())
            .addSubscriptions(Subscription.newBuilder().setProject("project1").setName("*").build())
            .build();
    Source source =
        Source.newBuilder()
            .setType(SourceType.KAFKA)
            .setKafkaSourceConfig(
                KafkaSourceConfig.newBuilder()
                    .setTopic("topic")
                    .setBootstrapServers("servers:9092")
                    .build())
            .build();

    FeatureSetProto.FeatureSet featureSetProto1 =
        FeatureSetProto.FeatureSet.newBuilder()
            .setSpec(
                FeatureSetSpec.newBuilder()
                    .setSource(source)
                    .setProject("project1")
                    .setName("features1"))
            .setMeta(FeatureSetMeta.newBuilder())
            .build();
    FeatureSet featureSet1 = FeatureSet.fromProto(featureSetProto1);
    FeatureSetProto.FeatureSet featureSetProto2 =
        FeatureSetProto.FeatureSet.newBuilder()
            .setSpec(
                FeatureSetSpec.newBuilder()
                    .setSource(source)
                    .setProject("project1")
                    .setName("features2"))
            .setMeta(FeatureSetMeta.newBuilder())
            .build();
    FeatureSet featureSet2 = FeatureSet.fromProto(featureSetProto2);
    String extId = "ext";
    ArgumentCaptor<List<Job>> jobArgCaptor = ArgumentCaptor.forClass(List.class);

    Job expectedInput =
        new Job(
            "",
            "",
            Runner.DATAFLOW,
            feast.core.model.Source.fromProto(source),
            feast.core.model.Store.fromProto(store),
            ModelHelpers.makeFeatureSetJobStatus(featureSet1, featureSet2),
            JobStatus.PENDING);

    Job expected =
        new Job(
            "some_id",
            extId,
            Runner.DATAFLOW,
            feast.core.model.Source.fromProto(source),
            feast.core.model.Store.fromProto(store),
            ModelHelpers.makeFeatureSetJobStatus(featureSet1, featureSet2),
            JobStatus.RUNNING);

    when(featureSetRepository.findAllByNameLikeAndProject_NameLikeOrderByNameAsc("%", "project1"))
        .thenReturn(Lists.newArrayList(featureSet1, featureSet2));
    when(specService.listStores(any()))
        .thenReturn(ListStoresResponse.newBuilder().addStore(store).build());

    when(jobManager.startJob(argThat(new JobMatcher(expectedInput)))).thenReturn(expected);
    when(jobManager.getRunnerType()).thenReturn(Runner.DATAFLOW);

    jcs.Poll();
    verify(jobRepository, times(1)).saveAll(jobArgCaptor.capture());
    List<Job> actual = jobArgCaptor.getValue();
    assertThat(actual, equalTo(Collections.singletonList(expected)));
  }

  @Test
  public void shouldGroupJobsBySource() throws InvalidProtocolBufferException {
    StoreProto.Store store =
        StoreProto.Store.newBuilder()
            .setName("test")
            .setType(StoreType.REDIS)
            .setRedisConfig(RedisConfig.newBuilder().build())
            .addSubscriptions(Subscription.newBuilder().setProject("project1").setName("*").build())
            .build();
    Source source1 =
        Source.newBuilder()
            .setType(SourceType.KAFKA)
            .setKafkaSourceConfig(
                KafkaSourceConfig.newBuilder()
                    .setTopic("topic")
                    .setBootstrapServers("servers:9092")
                    .build())
            .build();
    Source source2 =
        Source.newBuilder()
            .setType(SourceType.KAFKA)
            .setKafkaSourceConfig(
                KafkaSourceConfig.newBuilder()
                    .setTopic("topic")
                    .setBootstrapServers("other.servers:9092")
                    .build())
            .build();

    FeatureSetProto.FeatureSet featureSetProto1 =
        FeatureSetProto.FeatureSet.newBuilder()
            .setSpec(
                FeatureSetSpec.newBuilder()
                    .setSource(source1)
                    .setProject("project1")
                    .setName("features1"))
            .setMeta(FeatureSetMeta.newBuilder())
            .build();
    FeatureSet featureSet1 = FeatureSet.fromProto(featureSetProto1);

    FeatureSetProto.FeatureSet featureSetProto2 =
        FeatureSetProto.FeatureSet.newBuilder()
            .setSpec(
                FeatureSetSpec.newBuilder()
                    .setSource(source2)
                    .setProject("project1")
                    .setName("features2"))
            .setMeta(FeatureSetMeta.newBuilder())
            .build();
    FeatureSet featureSet2 = FeatureSet.fromProto(featureSetProto2);

    Job expectedInput1 =
        new Job(
            "name1",
            "",
            Runner.DATAFLOW,
            feast.core.model.Source.fromProto(source1),
            feast.core.model.Store.fromProto(store),
            ModelHelpers.makeFeatureSetJobStatus(featureSet1),
            JobStatus.PENDING);

    Job expected1 =
        new Job(
            "name1",
            "extId1",
            Runner.DATAFLOW,
            feast.core.model.Source.fromProto(source1),
            feast.core.model.Store.fromProto(store),
            ModelHelpers.makeFeatureSetJobStatus(featureSet1),
            JobStatus.RUNNING);

    Job expectedInput2 =
        new Job(
            "",
            "extId2",
            Runner.DATAFLOW,
            feast.core.model.Source.fromProto(source2),
            feast.core.model.Store.fromProto(store),
            ModelHelpers.makeFeatureSetJobStatus(featureSet2),
            JobStatus.PENDING);

    Job expected2 =
        new Job(
            "name2",
            "extId2",
            Runner.DATAFLOW,
            feast.core.model.Source.fromProto(source2),
            feast.core.model.Store.fromProto(store),
            ModelHelpers.makeFeatureSetJobStatus(featureSet2),
            JobStatus.RUNNING);
    ArgumentCaptor<List<Job>> jobArgCaptor = ArgumentCaptor.forClass(List.class);

    when(featureSetRepository.findAllByNameLikeAndProject_NameLikeOrderByNameAsc("%", "project1"))
        .thenReturn(Lists.newArrayList(featureSet1, featureSet2));

    when(specService.listStores(any()))
        .thenReturn(ListStoresResponse.newBuilder().addStore(store).build());

    when(jobManager.startJob(argThat(new JobMatcher(expectedInput1)))).thenReturn(expected1);
    when(jobManager.startJob(argThat(new JobMatcher(expectedInput2)))).thenReturn(expected2);
    when(jobManager.getRunnerType()).thenReturn(Runner.DATAFLOW);

    jcs.Poll();

    verify(jobRepository, times(1)).saveAll(jobArgCaptor.capture());
    List<Job> actual = jobArgCaptor.getValue();

    assertThat(actual.get(0), equalTo(expected1));
    assertThat(actual.get(1), equalTo(expected2));
  }

  @Test
  public void shouldSendPendingFeatureSetToJobs() {
    FeatureSet fs1 =
        TestObjectFactory.CreateFeatureSet(
            "fs_1", "project", Collections.emptyList(), Collections.emptyList());
    fs1.setVersion(2);

    FeatureSetJobStatus status1 =
        TestObjectFactory.CreateFeatureSetJobStatusWithJob(
            JobStatus.RUNNING, FeatureSetProto.FeatureSetJobDeliveryStatus.STATUS_DELIVERED, 1);
    FeatureSetJobStatus status2 =
        TestObjectFactory.CreateFeatureSetJobStatusWithJob(
            JobStatus.RUNNING, FeatureSetProto.FeatureSetJobDeliveryStatus.STATUS_DELIVERED, 1);
    FeatureSetJobStatus status3 =
        TestObjectFactory.CreateFeatureSetJobStatusWithJob(
            JobStatus.ABORTED, FeatureSetProto.FeatureSetJobDeliveryStatus.STATUS_DELIVERED, 2);

    // spec needs to be send
    fs1.getJobStatuses().addAll(ImmutableList.of(status1, status2, status3));

    FeatureSet fs2 =
        TestObjectFactory.CreateFeatureSet(
            "fs_2", "project", Collections.emptyList(), Collections.emptyList());
    fs2.setVersion(5);

    // spec already sent to kafka
    fs2.getJobStatuses()
        .addAll(
            ImmutableList.of(
                TestObjectFactory.CreateFeatureSetJobStatusWithJob(
                    JobStatus.RUNNING,
                    FeatureSetProto.FeatureSetJobDeliveryStatus.STATUS_IN_PROGRESS,
                    5)));

    // feature set without running jobs attached
    FeatureSet fs3 =
        TestObjectFactory.CreateFeatureSet(
            "fs_3", "project", Collections.emptyList(), Collections.emptyList());
    fs3.getJobStatuses()
        .addAll(
            ImmutableList.of(
                TestObjectFactory.CreateFeatureSetJobStatusWithJob(
                    JobStatus.ABORTED,
                    FeatureSetProto.FeatureSetJobDeliveryStatus.STATUS_IN_PROGRESS,
                    5)));

    when(featureSetRepository.findAllByStatus(FeatureSetProto.FeatureSetStatus.STATUS_PENDING))
        .thenReturn(ImmutableList.of(fs1, fs2, fs3));

    jcs.notifyJobsWhenFeatureSetUpdated();

    verify(kafkaTemplate).sendDefault(eq(fs1.getReference()), any(FeatureSetSpec.class));
    verify(kafkaTemplate, never()).sendDefault(eq(fs2.getReference()), any(FeatureSetSpec.class));
    verify(kafkaTemplate, never()).sendDefault(eq(fs3.getReference()), any(FeatureSetSpec.class));

    assertThat(status1.getVersion(), is(2));
    assertThat(
        status1.getDeliveryStatus(),
        is(FeatureSetProto.FeatureSetJobDeliveryStatus.STATUS_IN_PROGRESS));

    assertThat(status2.getVersion(), is(2));
    assertThat(
        status2.getDeliveryStatus(),
        is(FeatureSetProto.FeatureSetJobDeliveryStatus.STATUS_IN_PROGRESS));

    assertThat(status3.getVersion(), is(2));
    assertThat(
        status3.getDeliveryStatus(),
        is(FeatureSetProto.FeatureSetJobDeliveryStatus.STATUS_DELIVERED));
  }

  @Test
  @SneakyThrows
  public void shouldNotUpdateJobStatusVersionWhenKafkaUnavailable() {
    FeatureSet fsInTest =
        TestObjectFactory.CreateFeatureSet(
            "fs_1", "project", Collections.emptyList(), Collections.emptyList());
    fsInTest.setVersion(2);

    FeatureSetJobStatus featureSetJobStatus =
        TestObjectFactory.CreateFeatureSetJobStatusWithJob(
            JobStatus.RUNNING, FeatureSetProto.FeatureSetJobDeliveryStatus.STATUS_DELIVERED, 1);
    fsInTest.getJobStatuses().add(featureSetJobStatus);

    CancellationException exc = new CancellationException();
    when(kafkaTemplate.sendDefault(eq(fsInTest.getReference()), any()).get()).thenThrow(exc);
    when(featureSetRepository.findAllByStatus(FeatureSetProto.FeatureSetStatus.STATUS_PENDING))
        .thenReturn(ImmutableList.of(fsInTest));

    jcs.notifyJobsWhenFeatureSetUpdated();
    assertThat(featureSetJobStatus.getVersion(), is(1));
  }

  @Test
  public void specAckListenerShouldDoNothingWhenMessageIsOutdated() {
    FeatureSet fsInTest =
        TestObjectFactory.CreateFeatureSet(
            "fs", "project", Collections.emptyList(), Collections.emptyList());
    FeatureSetJobStatus j1 =
        TestObjectFactory.CreateFeatureSetJobStatusWithJob(
            JobStatus.RUNNING, FeatureSetProto.FeatureSetJobDeliveryStatus.STATUS_IN_PROGRESS, 1);
    FeatureSetJobStatus j2 =
        TestObjectFactory.CreateFeatureSetJobStatusWithJob(
            JobStatus.RUNNING, FeatureSetProto.FeatureSetJobDeliveryStatus.STATUS_IN_PROGRESS, 1);

    fsInTest.getJobStatuses().addAll(Arrays.asList(j1, j2));

    when(featureSetRepository.findFeatureSetByNameAndProject_Name(
            fsInTest.getName(), fsInTest.getProject().getName()))
        .thenReturn(fsInTest);

    jcs.listenAckFromJobs(newAckMessage("project/invalid", 0, j1.getJob().getId()));
    jcs.listenAckFromJobs(newAckMessage(fsInTest.getReference(), 0, ""));
    jcs.listenAckFromJobs(newAckMessage(fsInTest.getReference(), -1, j1.getJob().getId()));

    assertThat(
        j1.getDeliveryStatus(), is(FeatureSetProto.FeatureSetJobDeliveryStatus.STATUS_IN_PROGRESS));
    assertThat(
        j2.getDeliveryStatus(), is(FeatureSetProto.FeatureSetJobDeliveryStatus.STATUS_IN_PROGRESS));
  }

  @Test
  public void specAckListenerShouldUpdateFeatureSetStatus() {
    FeatureSet fsInTest =
        TestObjectFactory.CreateFeatureSet(
            "fs", "project", Collections.emptyList(), Collections.emptyList());
    fsInTest.setStatus(FeatureSetProto.FeatureSetStatus.STATUS_PENDING);

    FeatureSetJobStatus j1 =
        TestObjectFactory.CreateFeatureSetJobStatusWithJob(
            JobStatus.RUNNING, FeatureSetProto.FeatureSetJobDeliveryStatus.STATUS_IN_PROGRESS, 1);
    FeatureSetJobStatus j2 =
        TestObjectFactory.CreateFeatureSetJobStatusWithJob(
            JobStatus.RUNNING, FeatureSetProto.FeatureSetJobDeliveryStatus.STATUS_IN_PROGRESS, 1);
    FeatureSetJobStatus j3 =
        TestObjectFactory.CreateFeatureSetJobStatusWithJob(
            JobStatus.ABORTED, FeatureSetProto.FeatureSetJobDeliveryStatus.STATUS_IN_PROGRESS, 1);

    fsInTest.getJobStatuses().addAll(Arrays.asList(j1, j2, j3));

    when(featureSetRepository.findFeatureSetByNameAndProject_Name(
            fsInTest.getName(), fsInTest.getProject().getName()))
        .thenReturn(fsInTest);

    jcs.listenAckFromJobs(
        newAckMessage(fsInTest.getReference(), fsInTest.getVersion(), j1.getJob().getId()));

    assertThat(
        j1.getDeliveryStatus(), is(FeatureSetProto.FeatureSetJobDeliveryStatus.STATUS_DELIVERED));
    assertThat(fsInTest.getStatus(), is(FeatureSetProto.FeatureSetStatus.STATUS_PENDING));

    jcs.listenAckFromJobs(
        newAckMessage(fsInTest.getReference(), fsInTest.getVersion(), j2.getJob().getId()));

    assertThat(
        j2.getDeliveryStatus(), is(FeatureSetProto.FeatureSetJobDeliveryStatus.STATUS_DELIVERED));

    assertThat(fsInTest.getStatus(), is(FeatureSetProto.FeatureSetStatus.STATUS_READY));
  }

  private ConsumerRecord<String, IngestionJobProto.FeatureSetSpecAck> newAckMessage(
      String key, int version, String jobName) {
    return new ConsumerRecord<>(
        "topic",
        0,
        0,
        key,
        IngestionJobProto.FeatureSetSpecAck.newBuilder()
            .setFeatureSetVersion(version)
            .setJobName(jobName)
            .build());
  }
}
