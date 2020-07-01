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

import static feast.common.models.Store.convertStringToSubscription;
import static feast.proto.core.FeatureSetProto.FeatureSetJobDeliveryStatus.STATUS_DELIVERED;
import static feast.proto.core.FeatureSetProto.FeatureSetJobDeliveryStatus.STATUS_IN_PROGRESS;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.beans.HasPropertyWithValue.hasProperty;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.config.FeastProperties;
import feast.core.config.FeastProperties.JobProperties;
import feast.core.dao.FeatureSetJobStatusRepository;
import feast.core.dao.FeatureSetRepository;
import feast.core.dao.JobRepository;
import feast.core.dao.SourceRepository;
import feast.core.job.*;
import feast.core.model.*;
import feast.core.util.TestUtil;
import feast.proto.core.CoreServiceProto.ListFeatureSetsRequest.Filter;
import feast.proto.core.CoreServiceProto.ListFeatureSetsResponse;
import feast.proto.core.CoreServiceProto.ListStoresResponse;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.FeatureSetProto.FeatureSetMeta;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.IngestionJobProto;
import feast.proto.core.SourceProto;
import feast.proto.core.SourceProto.KafkaSourceConfig;
import feast.proto.core.SourceProto.SourceType;
import feast.proto.core.StoreProto;
import feast.proto.core.StoreProto.Store.Subscription;
import java.util.*;
import java.util.concurrent.CancellationException;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;
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
  @Mock FeatureSetJobStatusRepository jobStatusRepository;
  @Mock private KafkaTemplate<String, FeatureSetSpec> kafkaTemplate;
  @Mock SourceRepository sourceRepository;

  private FeastProperties feastProperties;
  private JobCoordinatorService jcsWithConsolidation;
  private JobCoordinatorService jcsWithJobPerStore;

  @Before
  public void setUp() {
    initMocks(this);
    feastProperties = new FeastProperties();
    JobProperties jobProperties = new JobProperties();
    jobProperties.setJobUpdateTimeoutSeconds(5);
    feastProperties.setJobs(jobProperties);

    jcsWithConsolidation =
        new JobCoordinatorService(
            jobRepository,
            featureSetRepository,
            jobStatusRepository,
            specService,
            jobManager,
            feastProperties,
            new ConsolidatedJobStrategy(jobRepository),
            kafkaTemplate);

    jcsWithJobPerStore =
        new JobCoordinatorService(
            jobRepository,
            featureSetRepository,
            jobStatusRepository,
            specService,
            jobManager,
            feastProperties,
            new JobPerStoreStrategy(jobRepository),
            kafkaTemplate);

    when(kafkaTemplate.sendDefault(any(), any())).thenReturn(new AsyncResult<>(null));
  }

  @Test
  public void shouldDoNothingIfNoStoresFound() throws InvalidProtocolBufferException {
    when(specService.listStores(any())).thenReturn(ListStoresResponse.newBuilder().build());
    jcsWithConsolidation.Poll();
    verify(jobRepository, times(0)).saveAndFlush(any());
  }

  @Test
  public void shouldDoNothingIfNoMatchingFeatureSetsFound() throws InvalidProtocolBufferException {
    Store store =
        TestUtil.createStore(
            "test", List.of(Subscription.newBuilder().setName("*").setProject("*").build()));
    StoreProto.Store storeSpec = store.toProto();

    when(specService.listStores(any()))
        .thenReturn(ListStoresResponse.newBuilder().addStore(storeSpec).build());
    when(specService.listFeatureSets(
            Filter.newBuilder().setProject("*").setFeatureSetName("*").build()))
        .thenReturn(ListFeatureSetsResponse.newBuilder().build());
    jcsWithConsolidation.Poll();
    verify(jobRepository, times(0)).saveAndFlush(any());
  }

  @Test
  public void shouldGenerateAndSubmitJobsIfAny() throws InvalidProtocolBufferException {
    Store store =
        TestUtil.createStore(
            "test", List.of(Subscription.newBuilder().setName("*").setProject("project1").build()));
    StoreProto.Store storeSpec = store.toProto();
    SourceProto.Source sourceSpec =
        SourceProto.Source.newBuilder()
            .setType(SourceType.KAFKA)
            .setKafkaSourceConfig(
                KafkaSourceConfig.newBuilder()
                    .setTopic("topic")
                    .setBootstrapServers("servers:9092")
                    .build())
            .build();
    Source source = Source.fromProto(sourceSpec);

    FeatureSetProto.FeatureSet featureSetSpec1 =
        FeatureSetProto.FeatureSet.newBuilder()
            .setSpec(
                FeatureSetSpec.newBuilder()
                    .setSource(sourceSpec)
                    .setProject("project1")
                    .setName("features1"))
            .setMeta(FeatureSetMeta.newBuilder())
            .build();
    FeatureSet featureSet1 = FeatureSet.fromProto(featureSetSpec1);
    FeatureSetProto.FeatureSet featureSetSpec2 =
        FeatureSetProto.FeatureSet.newBuilder()
            .setSpec(
                FeatureSetSpec.newBuilder()
                    .setSource(sourceSpec)
                    .setProject("project1")
                    .setName("features2"))
            .setMeta(FeatureSetMeta.newBuilder())
            .build();
    FeatureSet featureSet2 = FeatureSet.fromProto(featureSetSpec2);
    ArgumentCaptor<List<Job>> jobArgCaptor = ArgumentCaptor.forClass(List.class);

    Job expectedInput = newJob("id", store, source, featureSet1, featureSet2);

    Job expected =
        expectedInput.toBuilder().setExtId("extid1").setStatus(JobStatus.RUNNING).build();

    when(featureSetRepository.findAllByNameLikeAndProject_NameLikeOrderByNameAsc("%", "project1"))
        .thenReturn(Lists.newArrayList(featureSet1, featureSet2));
    when(sourceRepository.findFirstByTypeAndConfigOrderByIdAsc(
            source.getType(), source.getConfig()))
        .thenReturn(source);
    when(specService.listStores(any()))
        .thenReturn(ListStoresResponse.newBuilder().addStore(storeSpec).build());

    when(jobManager.startJob(expectedInput)).thenReturn(expected);
    when(jobManager.getRunnerType()).thenReturn(Runner.DATAFLOW);

    when(jobRepository.findByStatus(JobStatus.RUNNING)).thenReturn(List.of());
    when(jobRepository
            .findFirstBySourceTypeAndSourceConfigAndStoreNameAndStatusNotInOrderByLastUpdatedDesc(
                source.getType(),
                source.getConfig(),
                store.getName(),
                JobStatus.getTerminalStates()))
        .thenReturn(Optional.empty());

    jcsWithConsolidation.Poll();
    verify(jobRepository, times(1)).saveAll(jobArgCaptor.capture());
    List<Job> actual = jobArgCaptor.getValue();
    assertThat(actual, containsInAnyOrder(expected));
  }

  private Job newJob(String id, Store store, Source source, FeatureSet... featureSets) {
    Job job =
        Job.builder()
            .setId(id)
            .setExtId("")
            .setRunner(Runner.DATAFLOW)
            .setSource(source)
            .setFeatureSetJobStatuses(TestUtil.makeFeatureSetJobStatus(featureSets))
            .setStatus(JobStatus.PENDING)
            .build();
    job.setStores(ImmutableSet.of(store));
    return job;
  }

  @Test
  public void shouldGroupJobsBySource() throws InvalidProtocolBufferException {
    Store store =
        TestUtil.createStore(
            "test", List.of(Subscription.newBuilder().setName("*").setProject("project1").build()));
    StoreProto.Store storeSpec = store.toProto();

    Source source1 = TestUtil.createKafkaSource("servers:9092", "topic", false);
    Source source2 = TestUtil.createKafkaSource("others.servers:9092", "topic", false);

    FeatureSet featureSet1 = TestUtil.createEmptyFeatureSet("features1", source1);
    FeatureSet featureSet2 = TestUtil.createEmptyFeatureSet("features2", source2);

    Job expectedInput1 = newJob("id1", store, source1, featureSet1);

    Job expected1 =
        expectedInput1.toBuilder().setExtId("extid1").setStatus(JobStatus.RUNNING).build();

    Job expectedInput2 = newJob("id2", store, source2, featureSet2);

    Job expected2 =
        expectedInput2.toBuilder().setExtId("extid2").setStatus(JobStatus.RUNNING).build();

    when(featureSetRepository.findAllByNameLikeAndProject_NameLikeOrderByNameAsc("%", "project1"))
        .thenReturn(Lists.newArrayList(featureSet1, featureSet2));
    when(sourceRepository.findFirstByTypeAndConfigOrderByIdAsc(
            source1.getType(), source1.getConfig()))
        .thenReturn(source1);
    when(sourceRepository.findFirstByTypeAndConfigOrderByIdAsc(
            source2.getType(), source2.getConfig()))
        .thenReturn(source2);
    when(specService.listStores(any()))
        .thenReturn(ListStoresResponse.newBuilder().addStore(storeSpec).build());

    when(jobManager.startJob(expectedInput1)).thenReturn(expected1);
    when(jobManager.startJob(expectedInput2)).thenReturn(expected2);
    when(jobManager.getRunnerType()).thenReturn(Runner.DATAFLOW);

    when(jobRepository.findByStatus(JobStatus.RUNNING)).thenReturn(List.of());
    when(jobRepository
            .findFirstBySourceTypeAndSourceConfigAndStoreNameAndStatusNotInOrderByLastUpdatedDesc(
                source1.getType(), source1.getConfig(), null, JobStatus.getTerminalStates()))
        .thenReturn(Optional.empty());
    when(jobRepository
            .findFirstBySourceTypeAndSourceConfigAndStoreNameAndStatusNotInOrderByLastUpdatedDesc(
                source2.getType(), source2.getConfig(), null, JobStatus.getTerminalStates()))
        .thenReturn(Optional.empty());

    jcsWithConsolidation.Poll();

    ArgumentCaptor<List<Job>> jobArgCaptor = ArgumentCaptor.forClass(List.class);
    verify(jobRepository, times(1)).saveAll(jobArgCaptor.capture());
    List<Job> actual = jobArgCaptor.getValue();

    for (Job expectedJob : List.of(expected1, expected2)) {
      assertTrue(actual.contains(expectedJob));
    }
  }

  @Test
  public void shouldGroupJobsBySourceAndIgnoreDuplicateSourceObjects()
      throws InvalidProtocolBufferException {
    Store store =
        TestUtil.createStore(
            "test", List.of(Subscription.newBuilder().setName("*").setProject("project1").build()));
    StoreProto.Store storeSpec = store.toProto();

    // simulate duplicate source objects: create source objects from the same spec but with
    // different ids
    Source source1 = TestUtil.createKafkaSource("servers:9092", "topic", false);
    source1.setId(1);
    Source source2 = TestUtil.createKafkaSource("servers:9092", "topic", false);
    source2.setId(2);

    FeatureSet featureSet1 = TestUtil.createEmptyFeatureSet("features1", source1);
    FeatureSet featureSet2 = TestUtil.createEmptyFeatureSet("features2", source2);

    Job expectedInput = newJob("id", store, source1, featureSet1, featureSet2);

    Job expected =
        expectedInput.toBuilder().setExtId("extid1").setStatus(JobStatus.RUNNING).build();

    when(featureSetRepository.findAllByNameLikeAndProject_NameLikeOrderByNameAsc("%", "project1"))
        .thenReturn(Lists.newArrayList(featureSet1, featureSet2));
    when(sourceRepository.findFirstByTypeAndConfigOrderByIdAsc(
            source1.getType(), source1.getConfig()))
        .thenReturn(source1);
    when(sourceRepository.findFirstByTypeAndConfigOrderByIdAsc(
            source2.getType(), source2.getConfig()))
        .thenReturn(source1);
    when(specService.listStores(any()))
        .thenReturn(ListStoresResponse.newBuilder().addStore(storeSpec).build());

    when(jobManager.startJob(expectedInput)).thenReturn(expected);
    when(jobManager.getRunnerType()).thenReturn(Runner.DATAFLOW);

    when(jobRepository.findByStatus(JobStatus.RUNNING)).thenReturn(List.of());
    when(jobRepository
            .findFirstBySourceTypeAndSourceConfigAndStoreNameAndStatusNotInOrderByLastUpdatedDesc(
                source1.getType(), source1.getConfig(), null, JobStatus.getTerminalStates()))
        .thenReturn(Optional.empty());

    ArgumentCaptor<List<Job>> jobArgCaptor = ArgumentCaptor.forClass(List.class);

    jcsWithConsolidation.Poll();
    verify(jobRepository, times(1)).saveAll(jobArgCaptor.capture());
    List<Job> actual = jobArgCaptor.getValue();
    assertThat(actual, containsInAnyOrder(expected));
  }

  @Test
  public void shouldStopDuplicateJobsForSource() throws InvalidProtocolBufferException {
    Store store =
        TestUtil.createStore(
            "test", List.of(Subscription.newBuilder().setName("*").setProject("project1").build()));
    StoreProto.Store storeSpec = store.toProto();

    Source source = TestUtil.createKafkaSource("servers:9092", "topic", false);

    FeatureSet featureSet = TestUtil.createEmptyFeatureSet("features2", source);

    // simulate 3 running jobs, all serving the same source to store pairing.
    // JobCoordinatorService should abort 2 the extra jobs.
    List<Job> inputJobs = new ArrayList<>();
    List<Job> expectedJobs = new ArrayList<>();
    List<Job> extraJobs = new ArrayList();
    for (int i = 0; i < 3; i++) {
      Job job = newJob(String.format("id%d", i), store, source, featureSet);
      job.setExtId(String.format("extid%d", i));
      job.setStatus(JobStatus.RUNNING);
      inputJobs.add(job);

      JobStatus targetStatus = (i >= 1) ? JobStatus.ABORTED : JobStatus.RUNNING;
      expectedJobs.add(inputJobs.get(i).toBuilder().setStatus(targetStatus).build());

      if (targetStatus == JobStatus.ABORTED) {
        when(jobManager.abortJob(inputJobs.get(i))).thenReturn(expectedJobs.get(i));
        extraJobs.add(inputJobs.get(i));
      }
    }

    when(featureSetRepository.findAllByNameLikeAndProject_NameLikeOrderByNameAsc("%", "project1"))
        .thenReturn(Lists.newArrayList(featureSet));
    when(sourceRepository.findFirstByTypeAndConfigOrderByIdAsc(
            source.getType(), source.getConfig()))
        .thenReturn(source);
    when(specService.listStores(any()))
        .thenReturn(ListStoresResponse.newBuilder().addStore(storeSpec).build());

    when(jobManager.getJobStatus(inputJobs.get(0))).thenReturn(JobStatus.RUNNING);
    when(jobManager.getRunnerType()).thenReturn(Runner.DATAFLOW);

    when(jobRepository
            .findFirstBySourceTypeAndSourceConfigAndStoreNameAndStatusNotInOrderByLastUpdatedDesc(
                source.getType(), source.getConfig(), null, JobStatus.getTerminalStates()))
        .thenReturn(Optional.of(inputJobs.get(0)));
    when(jobRepository.findByStatus(JobStatus.RUNNING)).thenReturn(inputJobs);

    jcsWithConsolidation.Poll();

    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);

    verify(jobManager, times(2)).abortJob(jobCaptor.capture());
    List<Job> abortedJobs = jobCaptor.getAllValues();
    for (Job extraJob : extraJobs) {
      assertTrue(abortedJobs.contains(extraJob));
    }

    ArgumentCaptor<List<Job>> jobListCaptor = ArgumentCaptor.forClass(List.class);
    verify(jobRepository, times(1)).saveAll(jobListCaptor.capture());
    List<Job> actual = jobListCaptor.getValue();
    for (Job expectedJob : expectedJobs) {
      assertTrue(actual.contains(expectedJob));
    }
  }

  @Test
  public void shouldUseStoreSubscriptionToMapStore() throws InvalidProtocolBufferException {
    Store store1 =
        TestUtil.createStore(
            "test",
            List.of(Subscription.newBuilder().setName("features1").setProject("*").build()));
    StoreProto.Store store1Spec = store1.toProto();

    Store store2 =
        TestUtil.createStore(
            "test",
            List.of(Subscription.newBuilder().setName("features2").setProject("*").build()));
    StoreProto.Store store2Spec = store2.toProto();

    Source source1 = TestUtil.createKafkaSource("servers:9092", "topic", false);
    Source source2 = TestUtil.createKafkaSource("other.servers:9092", "topic", false);

    FeatureSet featureSet1 = TestUtil.createEmptyFeatureSet("feature1", source1);
    FeatureSet featureSet2 = TestUtil.createEmptyFeatureSet("feature2", source2);

    Job expectedInput1 = newJob("id1", store1, source1, featureSet1);

    Job expected1 =
        expectedInput1.toBuilder().setExtId("extid1").setStatus(JobStatus.RUNNING).build();

    Job expectedInput2 = newJob("id2", store2, source2, featureSet2);

    Job expected2 =
        expectedInput2.toBuilder().setExtId("extid2").setStatus(JobStatus.RUNNING).build();

    ArgumentCaptor<List<Job>> jobArgCaptor = ArgumentCaptor.forClass(List.class);

    when(featureSetRepository.findAllByNameLikeAndProject_NameLikeOrderByNameAsc("features1", "%"))
        .thenReturn(Lists.newArrayList(featureSet1));
    when(featureSetRepository.findAllByNameLikeAndProject_NameLikeOrderByNameAsc("features2", "%"))
        .thenReturn(Lists.newArrayList(featureSet2));
    when(sourceRepository.findFirstByTypeAndConfigOrderByIdAsc(
            source1.getType(), source1.getConfig()))
        .thenReturn(source1);
    when(sourceRepository.findFirstByTypeAndConfigOrderByIdAsc(
            source2.getType(), source2.getConfig()))
        .thenReturn(source2);
    when(specService.listStores(any()))
        .thenReturn(
            ListStoresResponse.newBuilder().addStore(store1Spec).addStore(store2Spec).build());

    when(jobManager.startJob(expectedInput1)).thenReturn(expected1);
    when(jobManager.startJob(expectedInput2)).thenReturn(expected2);
    when(jobManager.getRunnerType()).thenReturn(Runner.DATAFLOW);

    when(jobRepository.findByStatus(JobStatus.RUNNING)).thenReturn(List.of());
    when(jobRepository
            .findFirstBySourceTypeAndSourceConfigAndStoreNameAndStatusNotInOrderByLastUpdatedDesc(
                source1.getType(), source1.getConfig(), null, JobStatus.getTerminalStates()))
        .thenReturn(Optional.empty());
    when(jobRepository
            .findFirstBySourceTypeAndSourceConfigAndStoreNameAndStatusNotInOrderByLastUpdatedDesc(
                source2.getType(), source2.getConfig(), null, JobStatus.getTerminalStates()))
        .thenReturn(Optional.empty());

    jcsWithConsolidation.Poll();

    verify(jobRepository, times(1)).saveAll(jobArgCaptor.capture());
    List<Job> actual = jobArgCaptor.getValue();

    for (Job expectedJob : List.of(expected1, expected2)) {
      assertTrue(actual.contains(expectedJob));
    }
  }

  @Test
  public void shouldSendPendingFeatureSetToJobs() throws InvalidProtocolBufferException {
    FeatureSet fs1 =
        TestUtil.CreateFeatureSet(
            "fs_1", "project", Collections.emptyList(), Collections.emptyList());
    fs1.setVersion(2);

    FeatureSetJobStatus status1 =
        TestUtil.CreateFeatureSetJobStatusWithJob(JobStatus.RUNNING, STATUS_DELIVERED, 1);
    FeatureSetJobStatus status2 =
        TestUtil.CreateFeatureSetJobStatusWithJob(JobStatus.RUNNING, STATUS_DELIVERED, 1);
    FeatureSetJobStatus status3 =
        TestUtil.CreateFeatureSetJobStatusWithJob(JobStatus.ABORTED, STATUS_DELIVERED, 2);

    // spec needs to be send
    fs1.getJobStatuses().addAll(ImmutableList.of(status1, status2, status3));

    FeatureSet fs2 =
        TestUtil.CreateFeatureSet(
            "fs_2", "project", Collections.emptyList(), Collections.emptyList());
    fs2.setVersion(5);

    // spec already sent to kafka
    fs2.getJobStatuses()
        .addAll(
            ImmutableList.of(
                TestUtil.CreateFeatureSetJobStatusWithJob(
                    JobStatus.RUNNING, STATUS_IN_PROGRESS, 5)));

    // feature set without running jobs attached
    FeatureSet fs3 =
        TestUtil.CreateFeatureSet(
            "fs_3", "project", Collections.emptyList(), Collections.emptyList());
    fs3.getJobStatuses()
        .addAll(
            ImmutableList.of(
                TestUtil.CreateFeatureSetJobStatusWithJob(
                    JobStatus.ABORTED, STATUS_IN_PROGRESS, 5)));

    when(featureSetRepository.findAllByStatus(FeatureSetProto.FeatureSetStatus.STATUS_PENDING))
        .thenReturn(ImmutableList.of(fs1, fs2, fs3));

    when(specService.listStores(any())).thenReturn(ListStoresResponse.newBuilder().build());

    jcsWithConsolidation.notifyJobsWhenFeatureSetUpdated();

    verify(kafkaTemplate).sendDefault(eq(fs1.getReference()), any(FeatureSetSpec.class));
    verify(kafkaTemplate, never()).sendDefault(eq(fs2.getReference()), any(FeatureSetSpec.class));
    verify(kafkaTemplate, never()).sendDefault(eq(fs3.getReference()), any(FeatureSetSpec.class));

    assertThat(status1.getVersion(), is(2));
    assertThat(status1.getDeliveryStatus(), is(STATUS_IN_PROGRESS));

    assertThat(status2.getVersion(), is(2));
    assertThat(status2.getDeliveryStatus(), is(STATUS_IN_PROGRESS));

    assertThat(status3.getVersion(), is(2));
    assertThat(status3.getDeliveryStatus(), is(STATUS_DELIVERED));
  }

  @Test
  @SneakyThrows
  public void shouldNotUpdateJobStatusVersionWhenKafkaUnavailable() {
    FeatureSet fsInTest =
        TestUtil.CreateFeatureSet(
            "fs_1", "project", Collections.emptyList(), Collections.emptyList());
    fsInTest.setVersion(2);

    FeatureSetJobStatus featureSetJobStatus =
        TestUtil.CreateFeatureSetJobStatusWithJob(JobStatus.RUNNING, STATUS_DELIVERED, 1);
    fsInTest.getJobStatuses().add(featureSetJobStatus);

    CancellationException exc = new CancellationException();
    when(kafkaTemplate.sendDefault(eq(fsInTest.getReference()), any()).get()).thenThrow(exc);
    when(featureSetRepository.findAllByStatus(FeatureSetProto.FeatureSetStatus.STATUS_PENDING))
        .thenReturn(ImmutableList.of(fsInTest));
    when(specService.listStores(any())).thenReturn(ListStoresResponse.newBuilder().build());

    jcsWithConsolidation.notifyJobsWhenFeatureSetUpdated();
    assertThat(featureSetJobStatus.getVersion(), is(1));
  }

  @Test
  public void specAckListenerShouldDoNothingWhenMessageIsOutdated() {
    FeatureSet fsInTest =
        TestUtil.CreateFeatureSet(
            "fs", "project", Collections.emptyList(), Collections.emptyList());
    FeatureSetJobStatus j1 =
        TestUtil.CreateFeatureSetJobStatusWithJob(JobStatus.RUNNING, STATUS_IN_PROGRESS, 1);
    FeatureSetJobStatus j2 =
        TestUtil.CreateFeatureSetJobStatusWithJob(JobStatus.RUNNING, STATUS_IN_PROGRESS, 1);

    fsInTest.getJobStatuses().addAll(Arrays.asList(j1, j2));

    when(featureSetRepository.findFeatureSetByNameAndProject_Name(
            fsInTest.getName(), fsInTest.getProject().getName()))
        .thenReturn(fsInTest);

    jcsWithConsolidation.listenAckFromJobs(
        newAckMessage("project/invalid", 0, j1.getJob().getId()));
    jcsWithConsolidation.listenAckFromJobs(newAckMessage(fsInTest.getReference(), 0, ""));
    jcsWithConsolidation.listenAckFromJobs(
        newAckMessage(fsInTest.getReference(), -1, j1.getJob().getId()));

    assertThat(j1.getDeliveryStatus(), is(STATUS_IN_PROGRESS));
    assertThat(j2.getDeliveryStatus(), is(STATUS_IN_PROGRESS));
  }

  @Test
  public void specAckListenerShouldUpdateFeatureSetStatus() {
    FeatureSet fsInTest =
        TestUtil.CreateFeatureSet(
            "fs", "project", Collections.emptyList(), Collections.emptyList());
    fsInTest.setStatus(FeatureSetProto.FeatureSetStatus.STATUS_PENDING);

    FeatureSetJobStatus j1 =
        TestUtil.CreateFeatureSetJobStatusWithJob(JobStatus.RUNNING, STATUS_IN_PROGRESS, 1);
    FeatureSetJobStatus j2 =
        TestUtil.CreateFeatureSetJobStatusWithJob(JobStatus.RUNNING, STATUS_IN_PROGRESS, 1);
    FeatureSetJobStatus j3 =
        TestUtil.CreateFeatureSetJobStatusWithJob(JobStatus.ABORTED, STATUS_IN_PROGRESS, 1);

    fsInTest.getJobStatuses().addAll(Arrays.asList(j1, j2, j3));

    when(featureSetRepository.findFeatureSetByNameAndProject_Name(
            fsInTest.getName(), fsInTest.getProject().getName()))
        .thenReturn(fsInTest);

    jcsWithConsolidation.listenAckFromJobs(
        newAckMessage(fsInTest.getReference(), fsInTest.getVersion(), j1.getJob().getId()));

    assertThat(j1.getDeliveryStatus(), is(STATUS_DELIVERED));
    assertThat(fsInTest.getStatus(), is(FeatureSetProto.FeatureSetStatus.STATUS_PENDING));

    jcsWithConsolidation.listenAckFromJobs(
        newAckMessage(fsInTest.getReference(), fsInTest.getVersion(), j2.getJob().getId()));

    assertThat(j2.getDeliveryStatus(), is(STATUS_DELIVERED));

    assertThat(fsInTest.getStatus(), is(FeatureSetProto.FeatureSetStatus.STATUS_READY));
  }

  @Test
  public void featureSetShouldBeAllocated() throws InvalidProtocolBufferException {
    FeatureSetProto.FeatureSet.Builder fsBuilder =
        FeatureSetProto.FeatureSet.newBuilder().setMeta(FeatureSetMeta.newBuilder());
    FeatureSetSpec.Builder specBuilder = FeatureSetSpec.newBuilder();

    Source source = TestUtil.createKafkaSource("kafka", "topic", false);
    Store store =
        TestUtil.createStore("store", ImmutableList.of(convertStringToSubscription("*:*")));

    when(specService.listStores(any()))
        .thenReturn(ListStoresResponse.newBuilder().addStore(store.toProto()).build());

    FeatureSet featureSet1 =
        FeatureSet.fromProto(fsBuilder.setSpec(specBuilder.setName("featureSet1")).build());
    featureSet1.setSource(source);

    FeatureSetJobStatus status =
        TestUtil.CreateFeatureSetJobStatusWithJob(JobStatus.RUNNING, STATUS_IN_PROGRESS, 1);

    featureSet1.getJobStatuses().add(status);

    Job job = new Job();
    job.setStatus(JobStatus.RUNNING);

    when(jobRepository
            .findFirstBySourceTypeAndSourceConfigAndStoreNameAndStatusNotInOrderByLastUpdatedDesc(
                source.getType(), source.getConfig(), null, JobStatus.getTerminalStates()))
        .thenReturn(Optional.of(job));

    jcsWithConsolidation.allocateFeatureSetToJobs(featureSet1);

    FeatureSetJobStatus expectedStatus = new FeatureSetJobStatus();
    expectedStatus.setJob(job);
    expectedStatus.setFeatureSet(featureSet1);
    expectedStatus.setDeliveryStatus(STATUS_IN_PROGRESS);

    verify(jobStatusRepository).saveAll(ImmutableSet.of(expectedStatus));
    verify(jobStatusRepository).deleteAll(ImmutableSet.of(status));
  }

  @Test
  public void shouldCheckStatusOfAbortingJob() {
    Source source = TestUtil.createKafkaSource("kafka:9092", "topic", false);
    Store store = TestUtil.createStore("store", Collections.emptyList());

    Job job =
        Job.builder()
            .setStatus(JobStatus.ABORTING)
            .setFeatureSetJobStatuses(new HashSet<>())
            .setExtId("extId")
            .build();

    when(jobRepository
            .findFirstBySourceTypeAndSourceConfigAndStoreNameAndStatusNotInOrderByLastUpdatedDesc(
                source.getType(), source.getConfig(), null, JobStatus.getTerminalStates()))
        .thenReturn(Optional.of(job));

    List<JobTask> tasks =
        jcsWithConsolidation.makeJobUpdateTasks(
            ImmutableList.of(Pair.of(source, ImmutableSet.of(store))));

    assertThat("CheckStatus is expected", tasks.get(0) instanceof UpdateJobStatusTask);
  }

  @Test
  public void shouldUpgradeJobWhenNeeded() {
    Source source = TestUtil.createKafkaSource("kafka:9092", "topic", false);
    Store store = TestUtil.createStore("store", Collections.emptyList());

    Job job =
        Job.builder()
            .setStatus(JobStatus.RUNNING)
            .setFeatureSetJobStatuses(new HashSet<>())
            .setSource(source)
            .setExtId("extId")
            .setJobStores(new HashSet<>())
            .build();

    when(jobRepository
            .findFirstBySourceTypeAndSourceConfigAndStoreNameAndStatusNotInOrderByLastUpdatedDesc(
                source.getType(), source.getConfig(), null, JobStatus.getTerminalStates()))
        .thenReturn(Optional.of(job));

    List<JobTask> tasks =
        jcsWithConsolidation.makeJobUpdateTasks(
            ImmutableList.of(Pair.of(source, ImmutableSet.of(store))));

    assertThat("CreateTask is expected", tasks.get(0) instanceof CreateJobTask);
  }

  @Test
  public void shouldCreateJobIfNoRunning() {
    Source source = TestUtil.createKafkaSource("kafka:9092", "topic", false);
    Store store = TestUtil.createStore("store", Collections.emptyList());

    when(jobRepository
            .findFirstBySourceTypeAndSourceConfigAndStoreNameAndStatusNotInOrderByLastUpdatedDesc(
                source.getType(), source.getConfig(), null, JobStatus.getTerminalStates()))
        .thenReturn(Optional.empty());

    List<JobTask> tasks =
        jcsWithConsolidation.makeJobUpdateTasks(
            ImmutableList.of(Pair.of(source, ImmutableSet.of(store))));

    assertThat("CreateTask is expected", tasks.get(0) instanceof CreateJobTask);
  }

  @Test
  public void shouldCreateJobPerStore() throws InvalidProtocolBufferException {
    Store store1 =
        TestUtil.createStore(
            "test-1", List.of(Subscription.newBuilder().setName("*").setProject("*").build()));
    Store store2 =
        TestUtil.createStore(
            "test-2", List.of(Subscription.newBuilder().setName("*").setProject("*").build()));

    Source source = TestUtil.createKafkaSource("kafka", "topic", false);

    when(specService.listStores(any()))
        .thenReturn(
            ListStoresResponse.newBuilder()
                .addStore(store1.toProto())
                .addStore(store2.toProto())
                .build());

    when(featureSetRepository.findAllByNameLikeAndProject_NameLikeOrderByNameAsc("%", "%"))
        .thenReturn(ImmutableList.of(TestUtil.createEmptyFeatureSet("fs", source)));

    when(jobRepository
            .findFirstBySourceTypeAndSourceConfigAndStoreNameAndStatusNotInOrderByLastUpdatedDesc(
                eq(source.getType()),
                eq(source.getConfig()),
                any(),
                eq(JobStatus.getTerminalStates())))
        .thenReturn(Optional.empty());

    Job expected1 = newJob("", store1, source);
    Job expected2 = newJob("", store2, source);

    when(jobManager.startJob(any())).thenReturn(new Job());
    when(jobManager.getRunnerType()).thenReturn(Runner.DATAFLOW);

    jcsWithJobPerStore.Poll();

    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);

    verify(jobManager, times(2)).startJob(jobCaptor.capture());
    List<Job> actual = jobCaptor.getAllValues();

    assertThat(actual, containsInAnyOrder(expected1, expected2));
    assertThat(
        actual,
        hasItem(
            hasProperty(
                "id",
                containsString(
                    String.format("kafka-%d-to-test-1", Objects.hashCode(source.getConfig()))))));
    assertThat(
        actual,
        hasItem(
            hasProperty(
                "id",
                containsString(
                    String.format("kafka-%d-to-test-2", Objects.hashCode(source.getConfig()))))));
  }

  @Test
  public void shouldCloneRunningJobOnUpgrade() throws InvalidProtocolBufferException {
    Store store1 =
        TestUtil.createStore(
            "test-1", List.of(Subscription.newBuilder().setName("*").setProject("*").build()));
    Store store2 =
        TestUtil.createStore(
            "test-2", List.of(Subscription.newBuilder().setName("*").setProject("*").build()));

    Source source = TestUtil.createKafkaSource("kafka", "topic", false);

    when(specService.listStores(any()))
        .thenReturn(
            ListStoresResponse.newBuilder()
                .addStore(store1.toProto())
                .addStore(store2.toProto())
                .build());

    when(featureSetRepository.findAllByNameLikeAndProject_NameLikeOrderByNameAsc("%", "%"))
        .thenReturn(ImmutableList.of(TestUtil.createEmptyFeatureSet("fs", source)));

    Job existingJob = newJob("some-id", store1, source);
    existingJob.setExtId("extId");
    existingJob.setFeatureSetJobStatuses(new HashSet<>());
    existingJob.setStatus(JobStatus.RUNNING);

    when(jobRepository
            .findFirstBySourceTypeAndSourceConfigAndStoreNameAndStatusNotInOrderByLastUpdatedDesc(
                eq(source.getType()),
                eq(source.getConfig()),
                any(),
                eq(JobStatus.getTerminalStates())))
        .thenReturn(Optional.of(existingJob));

    when(jobManager.getRunnerType()).thenReturn(Runner.DATAFLOW);

    jcsWithConsolidation.Poll();

    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);

    // not stopped yet
    verify(jobManager, never()).abortJob(any());

    verify(jobManager, times(1)).startJob(jobCaptor.capture());

    Job actual = jobCaptor.getValue();
    assertThat(actual.getId(), not("some-id"));
    assertThat(actual.getSource(), equalTo(existingJob.getSource()));
    assertThat(actual.getStores(), containsInAnyOrder(store1, store2));
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
