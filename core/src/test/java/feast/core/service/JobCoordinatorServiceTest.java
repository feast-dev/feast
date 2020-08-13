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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.beans.HasPropertyWithValue.hasProperty;
import static org.hamcrest.core.Is.isA;
import static org.hamcrest.core.IsIterableContaining.hasItem;
import static org.hamcrest.core.StringContains.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.collect.*;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.config.FeastProperties;
import feast.core.config.FeastProperties.JobProperties;
import feast.core.it.DataGenerator;
import feast.core.job.*;
import feast.core.job.JobRepository;
import feast.core.job.task.*;
import feast.core.model.Job;
import feast.core.model.JobStatus;
import feast.core.util.TestUtil;
import feast.proto.core.CoreServiceProto.ListFeatureSetsRequest.Filter;
import feast.proto.core.CoreServiceProto.ListFeatureSetsResponse;
import feast.proto.core.CoreServiceProto.ListStoresResponse;
import feast.proto.core.FeatureSetProto.FeatureSet;
import feast.proto.core.SourceProto.Source;
import feast.proto.core.StoreProto.Store;
import java.util.*;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.springframework.kafka.core.KafkaTemplate;

public class JobCoordinatorServiceTest {

  @Rule public final ExpectedException exception = ExpectedException.none();

  JobRepository jobRepository;

  @Mock SpecService specService;

  private FeastProperties feastProperties;
  private JobCoordinatorService jcsWithConsolidation;
  private JobCoordinatorService jcsWithJobPerStore;

  @Before
  public void setUp() {
    initMocks(this);
    feastProperties = new FeastProperties();
    JobProperties jobProperties = new JobProperties();
    jobProperties.setJobUpdateTimeoutSeconds(5);

    JobProperties.CoordinatorProperties.FeatureSetSelector selector =
        new JobProperties.CoordinatorProperties.FeatureSetSelector();
    selector.setName("fs*");
    selector.setProject("*");

    JobProperties.CoordinatorProperties coordinatorProperties =
        new JobProperties.CoordinatorProperties();
    coordinatorProperties.setFeatureSetSelector(ImmutableList.of(selector));
    coordinatorProperties.setWhitelistedStores(
        ImmutableList.of("test-store", "test", "test-1", "test-2", "normal-store"));

    jobProperties.setCoordinator(coordinatorProperties);
    feastProperties.setJobs(jobProperties);

    TestUtil.setupAuditLogger();

    JobManager jobManager = mock(JobManager.class);

    when(jobManager.listRunningJobs()).thenReturn(Collections.emptyList());
    jobRepository = new InMemoryJobRepository(jobManager);

    jcsWithConsolidation =
        new JobCoordinatorService(
            jobRepository,
            specService,
            jobManager,
            feastProperties,
            new ConsolidatedJobStrategy(jobRepository),
            mock(KafkaTemplate.class));

    jcsWithJobPerStore =
        new JobCoordinatorService(
            jobRepository,
            specService,
            jobManager,
            feastProperties,
            new JobPerStoreStrategy(jobRepository),
            mock(KafkaTemplate.class));
  }

  @Test
  public void shouldDoNothingIfNoStoresFound() {
    when(specService.listStores(any())).thenReturn(ListStoresResponse.newBuilder().build());

    List<JobTask> jobTasks =
        jcsWithConsolidation.makeJobUpdateTasks(jcsWithConsolidation.getSourceToStoreMappings());

    assertThat(jobTasks, hasSize(0));
  }

  @Test
  public void shouldDoNothingIfNoMatchingFeatureSetsFound() throws InvalidProtocolBufferException {
    Store storeSpec = DataGenerator.getDefaultStore();

    when(specService.listStores(any()))
        .thenReturn(ListStoresResponse.newBuilder().addStore(storeSpec).build());
    when(specService.listFeatureSets(
            Filter.newBuilder().setProject("*").setFeatureSetName("*").build()))
        .thenReturn(ListFeatureSetsResponse.newBuilder().build());

    List<JobTask> jobTasks =
        jcsWithConsolidation.makeJobUpdateTasks(jcsWithConsolidation.getSourceToStoreMappings());

    assertThat(jobTasks, hasSize(0));
  }

  @Test
  @SneakyThrows
  public void shouldGroupJobsBySource() {
    Store store =
        DataGenerator.createStore(
            "test", Store.StoreType.REDIS, ImmutableList.of(Triple.of("project1", "*", false)));

    Source source1 = DataGenerator.createSource("servers:9092", "topic");
    Source source2 = DataGenerator.createSource("others.servers:9092", "topic");

    FeatureSet featureSet1 = DataGenerator.createFeatureSet(source1, "project1", "fs1");
    FeatureSet featureSet2 = DataGenerator.createFeatureSet(source2, "project1", "fs2");

    when(specService.listFeatureSets(
            Filter.newBuilder().setFeatureSetName("*").setProject("project1").build()))
        .thenReturn(
            ListFeatureSetsResponse.newBuilder()
                .addAllFeatureSets(Lists.newArrayList(featureSet1, featureSet2))
                .build());
    when(specService.listStores(any()))
        .thenReturn(ListStoresResponse.newBuilder().addStore(store).build());

    ArrayList<Pair<Source, Set<Store>>> pairs =
        Lists.newArrayList(jcsWithConsolidation.getSourceToStoreMappings());

    assertThat(pairs, hasSize(2));
    assertThat(pairs, hasItem(Pair.of(source1, Sets.newHashSet(store))));
    assertThat(pairs, hasItem(Pair.of(source2, Sets.newHashSet(store))));
  }

  @Test
  @SneakyThrows
  public void shouldUseStoreSubscriptionToMapStore() {
    Store store1 =
        DataGenerator.createStore(
            "test", Store.StoreType.REDIS, ImmutableList.of(Triple.of("*", "features1", false)));

    Store store2 =
        DataGenerator.createStore(
            "test", Store.StoreType.REDIS, ImmutableList.of(Triple.of("*", "features2", false)));

    Source source1 = DataGenerator.createSource("servers:9092", "topic");
    Source source2 = DataGenerator.createSource("other.servers:9092", "topic");

    FeatureSet featureSet1 = DataGenerator.createFeatureSet(source1, "default", "fs1");
    FeatureSet featureSet2 = DataGenerator.createFeatureSet(source2, "default", "fs2");

    when(specService.listFeatureSets(
            Filter.newBuilder().setFeatureSetName("features1").setProject("*").build()))
        .thenReturn(
            ListFeatureSetsResponse.newBuilder()
                .addAllFeatureSets(Lists.newArrayList(featureSet1))
                .build());

    when(specService.listFeatureSets(
            Filter.newBuilder().setFeatureSetName("features2").setProject("*").build()))
        .thenReturn(
            ListFeatureSetsResponse.newBuilder()
                .addAllFeatureSets(Lists.newArrayList(featureSet2))
                .build());

    when(specService.listStores(any()))
        .thenReturn(ListStoresResponse.newBuilder().addStore(store1).addStore(store2).build());

    ArrayList<Pair<Source, Set<Store>>> pairs =
        Lists.newArrayList(jcsWithConsolidation.getSourceToStoreMappings());

    assertThat(pairs, hasSize(2));
    assertThat(pairs, hasItem(Pair.of(source1, Sets.newHashSet(store1))));
    assertThat(pairs, hasItem(Pair.of(source2, Sets.newHashSet(store2))));
  }

  @Test
  public void shouldCheckStatusOfAbortingJob() {
    Source source = DataGenerator.createSource("kafka:9092", "topic");
    Store store = DataGenerator.getDefaultStore();

    Job job =
        Job.builder()
            .setSource(source)
            .setStores(ImmutableMap.of(store.getName(), store))
            .setId("some-id")
            .build();
    job.setStatus(JobStatus.ABORTING);
    job.setExtId("extId");

    jobRepository.add(job);

    List<JobTask> tasks =
        jcsWithConsolidation.makeJobUpdateTasks(
            ImmutableList.of(Pair.of(source, ImmutableSet.of(store))));

    assertThat("CheckStatus is expected", tasks.get(0) instanceof UpdateJobStatusTask);
  }

  @Test
  public void shouldUpgradeJobWhenNeeded() {
    Source source = DataGenerator.createSource("kafka:9092", "topic");
    Store store = DataGenerator.getDefaultStore();

    Job job = Job.builder().setSource(source).setId("some-id").build();

    job.setStatus(JobStatus.RUNNING);
    job.setExtId("extId");

    jobRepository.add(job);

    List<JobTask> tasks =
        jcsWithConsolidation.makeJobUpdateTasks(
            ImmutableList.of(Pair.of(source, ImmutableSet.of(store))));

    assertThat("CreateTask is expected", tasks.get(0) instanceof CreateJobTask);
  }

  @Test
  @SneakyThrows
  public void shouldCreateJobIfNoRunning() {
    Source source = DataGenerator.createSource("kafka:9092", "topic");
    Store store = DataGenerator.getDefaultStore();

    when(specService.listFeatureSets(
            Filter.newBuilder().setFeatureSetName("*").setProject("*").build()))
        .thenReturn(ListFeatureSetsResponse.newBuilder().build());

    List<JobTask> tasks =
        jcsWithConsolidation.makeJobUpdateTasks(
            ImmutableList.of(Pair.of(source, ImmutableSet.of(store))));

    assertThat("CreateTask is expected", tasks.get(0) instanceof CreateJobTask);
  }

  @Test
  public void shouldCreateJobPerStore() throws InvalidProtocolBufferException {
    Store store1 =
        DataGenerator.createStore(
            "test-1", Store.StoreType.REDIS, ImmutableList.of(Triple.of("*", "*", false)));
    Store store2 =
        DataGenerator.createStore(
            "test-2", Store.StoreType.REDIS, ImmutableList.of(Triple.of("*", "*", false)));

    Source source = DataGenerator.createSource("servers:9092", "topic");

    FeatureSet featureSet = DataGenerator.createFeatureSet(source, "default", "fs1");

    when(specService.listFeatureSets(
            Filter.newBuilder().setFeatureSetName("*").setProject("*").build()))
        .thenReturn(
            ListFeatureSetsResponse.newBuilder()
                .addAllFeatureSets(Lists.newArrayList(featureSet))
                .build());
    when(specService.listStores(any()))
        .thenReturn(ListStoresResponse.newBuilder().addStore(store1).addStore(store2).build());

    List<JobTask> jobTasks =
        jcsWithJobPerStore.makeJobUpdateTasks(jcsWithJobPerStore.getSourceToStoreMappings());

    int hash =
        Objects.hash(
            source.getKafkaSourceConfig().getBootstrapServers(),
            source.getKafkaSourceConfig().getTopic());

    assertThat(jobTasks, hasSize(2));
    assertThat(
        jobTasks,
        hasItem(
            hasProperty(
                "job",
                hasProperty("id", containsString(String.format("kafka-%d-to-test-1", hash))))));
    assertThat(
        jobTasks,
        hasItem(
            hasProperty(
                "job",
                hasProperty("id", containsString(String.format("kafka-%d-to-test-2", hash))))));
  }

  @Test
  public void shouldCloneRunningJobOnUpgrade() throws InvalidProtocolBufferException {
    Store store1 =
        DataGenerator.createStore(
            "test-1", Store.StoreType.REDIS, ImmutableList.of(Triple.of("*", "*", false)));
    Store store2 =
        DataGenerator.createStore(
            "test-2", Store.StoreType.REDIS, ImmutableList.of(Triple.of("*", "*", false)));

    Source source = DataGenerator.createSource("servers:9092", "topic");

    Job existingJob =
        Job.builder()
            .setId("some-id")
            .setSource(source)
            .setStores(ImmutableMap.of(store1.getName(), store1))
            .build();

    existingJob.setExtId("extId");
    existingJob.setStatus(JobStatus.RUNNING);

    jobRepository.add(existingJob);

    List<JobTask> jobTasks =
        jcsWithConsolidation.makeJobUpdateTasks(
            ImmutableList.of(Pair.of(source, ImmutableSet.of(store1, store2))));

    assertThat(jobTasks, hasSize(1));
    assertThat(jobTasks, hasItem(isA(CreateJobTask.class)));
  }

  @Test
  @SneakyThrows
  public void shouldSelectOnlyFeatureSetsThatJobManagerSubscribedTo() {
    Store store = DataGenerator.getDefaultStore();
    Source source = DataGenerator.getDefaultSource();

    FeatureSet featureSet1 = DataGenerator.createFeatureSet(source, "default", "fs1");
    FeatureSet featureSet2 = DataGenerator.createFeatureSet(source, "project", "fs3");
    FeatureSet featureSet3 = DataGenerator.createFeatureSet(source, "default", "not-fs");

    when(specService.listFeatureSets(
            Filter.newBuilder().setFeatureSetName("*").setProject("*").build()))
        .thenReturn(
            ListFeatureSetsResponse.newBuilder()
                .addAllFeatureSets(Lists.newArrayList(featureSet1, featureSet2, featureSet3))
                .build());

    List<FeatureSet> featureSetsForStore = jcsWithConsolidation.getFeatureSetsForStore(store);
    assertThat(featureSetsForStore, containsInAnyOrder(featureSet1, featureSet2));
  }

  @Test
  @SneakyThrows
  public void shouldSelectOnlyStoresThatNotBlacklisted() {
    Store store1 =
        DataGenerator.createStore(
            "normal-store",
            Store.StoreType.REDIS,
            ImmutableList.of(Triple.of("project1", "*", false)));
    Store store2 =
        DataGenerator.createStore(
            "blacklisted-store",
            Store.StoreType.REDIS,
            ImmutableList.of(Triple.of("project2", "*", false)));

    Source source1 = DataGenerator.createSource("source-1", "topic");
    Source source2 = DataGenerator.createSource("source-2", "topic");

    FeatureSet featureSet1 = DataGenerator.createFeatureSet(source1, "default", "fs1");
    FeatureSet featureSet2 = DataGenerator.createFeatureSet(source2, "project", "fs3");

    when(specService.listStores(any()))
        .thenReturn(ListStoresResponse.newBuilder().addStore(store1).addStore(store2).build());

    when(specService.listFeatureSets(
            Filter.newBuilder().setProject("project1").setFeatureSetName("*").build()))
        .thenReturn(ListFeatureSetsResponse.newBuilder().addFeatureSets(featureSet1).build());

    when(specService.listFeatureSets(
            Filter.newBuilder().setProject("project2").setFeatureSetName("*").build()))
        .thenReturn(ListFeatureSetsResponse.newBuilder().addFeatureSets(featureSet2).build());

    ArrayList<Pair<Source, Set<Store>>> pairs =
        Lists.newArrayList(jcsWithConsolidation.getSourceToStoreMappings());

    assertThat(pairs, containsInAnyOrder(Pair.of(source1, ImmutableSet.of(store1))));
  }
}
