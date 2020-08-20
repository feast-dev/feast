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
package feast.jobcontroller.service;

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
import feast.common.it.DataGenerator;
import feast.jobcontroller.config.FeastProperties;
import feast.jobcontroller.config.FeastProperties.JobProperties;
import feast.jobcontroller.dao.InMemoryJobRepository;
import feast.jobcontroller.dao.JobRepository;
import feast.jobcontroller.model.Job;
import feast.jobcontroller.model.JobStatus;
import feast.jobcontroller.runner.ConsolidatedJobStrategy;
import feast.jobcontroller.runner.JobManager;
import feast.jobcontroller.runner.JobPerStoreStrategy;
import feast.jobcontroller.runner.task.CreateJobTask;
import feast.jobcontroller.runner.task.JobTask;
import feast.jobcontroller.runner.task.UpdateJobStatusTask;
import feast.proto.core.CoreServiceGrpc;
import feast.proto.core.CoreServiceProto;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.kafka.core.KafkaTemplate;

public class JobControllerServiceTest {

  JobRepository jobRepository;

  @Mock CoreServiceGrpc.CoreServiceBlockingStub specService;

  private FeastProperties feastProperties;
  private JobControllerService jobcontrollersWithConsolidation;
  private JobControllerService jobcontrollersWithJobPerStore;

  @BeforeEach
  public void setUp() {
    initMocks(this);
    feastProperties = new FeastProperties();
    JobProperties jobProperties = new JobProperties();
    jobProperties.setJobUpdateTimeoutSeconds(5);

    FeastProperties.JobProperties.ControllerProperties.FeatureSetSelector selector =
        new FeastProperties.JobProperties.ControllerProperties.FeatureSetSelector();
    selector.setName("fs*");
    selector.setProject("*");

    FeastProperties.JobProperties.ControllerProperties controllerProperties =
        new FeastProperties.JobProperties.ControllerProperties();
    controllerProperties.setFeatureSetSelector(ImmutableList.of(selector));
    controllerProperties.setWhitelistedStores(
        ImmutableList.of("test-store", "test", "test-1", "test-2", "normal-store"));
    controllerProperties.setJobSelector(ImmutableMap.of("application", "feast"));

    jobProperties.setController(controllerProperties);
    feastProperties.setJobs(jobProperties);
    feastProperties.setVersion("1.0.0");

    JobManager jobManager = mock(JobManager.class);

    when(jobManager.listRunningJobs()).thenReturn(Collections.emptyList());
    jobRepository = new InMemoryJobRepository(jobManager);

    jobcontrollersWithConsolidation =
        new JobControllerService(
            jobRepository,
            specService,
            jobManager,
            feastProperties,
            new ConsolidatedJobStrategy(jobRepository),
            mock(KafkaTemplate.class));

    jobcontrollersWithJobPerStore =
        new JobControllerService(
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
        jobcontrollersWithConsolidation.makeJobUpdateTasks(jobcontrollersWithConsolidation.getSourceToStoreMappings());

    assertThat(jobTasks, hasSize(0));
  }

  @Test
  public void shouldDoNothingIfNoMatchingFeatureSetsFound() throws InvalidProtocolBufferException {
    Store storeSpec = DataGenerator.getDefaultStore();

    when(specService.listStores(any()))
        .thenReturn(ListStoresResponse.newBuilder().addStore(storeSpec).build());
    when(specService.listFeatureSets(
            CoreServiceProto.ListFeatureSetsRequest.newBuilder()
                .setFilter(Filter.newBuilder().setProject("*").setFeatureSetName("*").build())
                .build()))
        .thenReturn(ListFeatureSetsResponse.newBuilder().build());

    List<JobTask> jobTasks =
        jobcontrollersWithConsolidation.makeJobUpdateTasks(jobcontrollersWithConsolidation.getSourceToStoreMappings());

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
            CoreServiceProto.ListFeatureSetsRequest.newBuilder()
                .setFilter(
                    Filter.newBuilder().setFeatureSetName("*").setProject("project1").build())
                .build()))
        .thenReturn(
            ListFeatureSetsResponse.newBuilder()
                .addAllFeatureSets(Lists.newArrayList(featureSet1, featureSet2))
                .build());
    when(specService.listStores(any()))
        .thenReturn(ListStoresResponse.newBuilder().addStore(store).build());

    ArrayList<Pair<Source, Set<Store>>> pairs =
        Lists.newArrayList(jobcontrollersWithConsolidation.getSourceToStoreMappings());

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
            CoreServiceProto.ListFeatureSetsRequest.newBuilder()
                .setFilter(
                    Filter.newBuilder().setFeatureSetName("features1").setProject("*").build())
                .build()))
        .thenReturn(
            ListFeatureSetsResponse.newBuilder()
                .addAllFeatureSets(Lists.newArrayList(featureSet1))
                .build());

    when(specService.listFeatureSets(
            CoreServiceProto.ListFeatureSetsRequest.newBuilder()
                .setFilter(
                    Filter.newBuilder().setFeatureSetName("features2").setProject("*").build())
                .build()))
        .thenReturn(
            ListFeatureSetsResponse.newBuilder()
                .addAllFeatureSets(Lists.newArrayList(featureSet2))
                .build());

    when(specService.listStores(any()))
        .thenReturn(ListStoresResponse.newBuilder().addStore(store1).addStore(store2).build());

    ArrayList<Pair<Source, Set<Store>>> pairs =
        Lists.newArrayList(jobcontrollersWithConsolidation.getSourceToStoreMappings());

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
        jobcontrollersWithConsolidation.makeJobUpdateTasks(
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
        jobcontrollersWithConsolidation.makeJobUpdateTasks(
            ImmutableList.of(Pair.of(source, ImmutableSet.of(store))));

    assertThat("CreateTask is expected", tasks.get(0) instanceof CreateJobTask);
  }

  @Test
  @SneakyThrows
  public void shouldCreateJobIfNoRunning() {
    Source source = DataGenerator.createSource("kafka:9092", "topic");
    Store store = DataGenerator.getDefaultStore();

    when(specService.listFeatureSets(
            CoreServiceProto.ListFeatureSetsRequest.newBuilder()
                .setFilter(Filter.newBuilder().setFeatureSetName("*").setProject("*").build())
                .build()))
        .thenReturn(ListFeatureSetsResponse.newBuilder().build());

    List<JobTask> tasks =
        jobcontrollersWithConsolidation.makeJobUpdateTasks(
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
            CoreServiceProto.ListFeatureSetsRequest.newBuilder()
                .setFilter(Filter.newBuilder().setFeatureSetName("*").setProject("*").build())
                .build()))
        .thenReturn(
            ListFeatureSetsResponse.newBuilder()
                .addAllFeatureSets(Lists.newArrayList(featureSet))
                .build());
    when(specService.listStores(any()))
        .thenReturn(ListStoresResponse.newBuilder().addStore(store1).addStore(store2).build());

    List<JobTask> jobTasks =
        jobcontrollersWithJobPerStore.makeJobUpdateTasks(jobcontrollersWithJobPerStore.getSourceToStoreMappings());

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
        jobcontrollersWithConsolidation.makeJobUpdateTasks(
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
            CoreServiceProto.ListFeatureSetsRequest.newBuilder()
                .setFilter(Filter.newBuilder().setFeatureSetName("*").setProject("*").build())
                .build()))
        .thenReturn(
            ListFeatureSetsResponse.newBuilder()
                .addAllFeatureSets(Lists.newArrayList(featureSet1, featureSet2, featureSet3))
                .build());

    List<FeatureSet> featureSetsForStore = jobcontrollersWithConsolidation.getFeatureSetsForStore(store);
    assertThat(featureSetsForStore, containsInAnyOrder(featureSet1, featureSet2));
  }

  @Test
  @SneakyThrows
  public void shouldSelectOnlyStoresThatWhitelisted() {
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
            CoreServiceProto.ListFeatureSetsRequest.newBuilder()
                .setFilter(
                    Filter.newBuilder().setProject("project1").setFeatureSetName("*").build())
                .build()))
        .thenReturn(ListFeatureSetsResponse.newBuilder().addFeatureSets(featureSet1).build());

    when(specService.listFeatureSets(
            CoreServiceProto.ListFeatureSetsRequest.newBuilder()
                .setFilter(
                    Filter.newBuilder().setProject("project2").setFeatureSetName("*").build())
                .build()))
        .thenReturn(ListFeatureSetsResponse.newBuilder().addFeatureSets(featureSet2).build());

    ArrayList<Pair<Source, Set<Store>>> pairs =
        Lists.newArrayList(jobcontrollersWithConsolidation.getSourceToStoreMappings());

    assertThat(pairs, containsInAnyOrder(Pair.of(source1, ImmutableSet.of(store1))));
  }
}
