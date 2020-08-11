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
import feast.core.dao.FeatureSetRepository;
import feast.core.it.DataGenerator;
import feast.core.job.*;
import feast.core.job.JobRepository;
import feast.core.job.task.*;
import feast.core.model.FeatureSet;
import feast.core.model.Job;
import feast.core.model.JobStatus;
import feast.core.util.TestUtil;
import feast.proto.core.CoreServiceProto.ListFeatureSetsRequest.Filter;
import feast.proto.core.CoreServiceProto.ListFeatureSetsResponse;
import feast.proto.core.CoreServiceProto.ListStoresResponse;
import feast.proto.core.SourceProto.Source;
import feast.proto.core.StoreProto.Store;
import java.util.*;
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
  @Mock FeatureSetRepository featureSetRepository;

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
    TestUtil.setupAuditLogger();

    JobManager jobManager = mock(JobManager.class);

    when(jobManager.listRunningJobs()).thenReturn(Collections.emptyList());
    jobRepository = new InMemoryJobRepository(jobManager);

    jcsWithConsolidation =
        new JobCoordinatorService(
            jobRepository,
            featureSetRepository,
            specService,
            jobManager,
            feastProperties,
            new ConsolidatedJobStrategy(jobRepository),
            mock(KafkaTemplate.class));

    jcsWithJobPerStore =
        new JobCoordinatorService(
            jobRepository,
            featureSetRepository,
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
  public void shouldGroupJobsBySource() {
    Store store =
        DataGenerator.createStore(
            "test", Store.StoreType.REDIS, ImmutableList.of(Triple.of("project1", "*", false)));

    Source source1 = DataGenerator.createSource("servers:9092", "topic");
    Source source2 = DataGenerator.createSource("others.servers:9092", "topic");

    FeatureSet featureSet1 =
        TestUtil.createEmptyFeatureSet("features1", feast.core.model.Source.fromProto(source1));
    FeatureSet featureSet2 =
        TestUtil.createEmptyFeatureSet("features2", feast.core.model.Source.fromProto(source2));

    when(featureSetRepository.findAllByNameLikeAndProject_NameLikeOrderByNameAsc("%", "project1"))
        .thenReturn(Lists.newArrayList(featureSet1, featureSet2));
    when(specService.listStores(any()))
        .thenReturn(ListStoresResponse.newBuilder().addStore(store).build());

    ArrayList<Pair<Source, Set<Store>>> pairs =
        Lists.newArrayList(jcsWithConsolidation.getSourceToStoreMappings());

    assertThat(pairs, hasSize(2));
    assertThat(pairs, hasItem(Pair.of(source1, Sets.newHashSet(store))));
    assertThat(pairs, hasItem(Pair.of(source2, Sets.newHashSet(store))));
  }

  @Test
  public void shouldUseStoreSubscriptionToMapStore() {
    Store store1 =
        DataGenerator.createStore(
            "test", Store.StoreType.REDIS, ImmutableList.of(Triple.of("*", "features1", false)));

    Store store2 =
        DataGenerator.createStore(
            "test", Store.StoreType.REDIS, ImmutableList.of(Triple.of("*", "features2", false)));

    Source source1 = DataGenerator.createSource("servers:9092", "topic");
    Source source2 = DataGenerator.createSource("other.servers:9092", "topic");

    FeatureSet featureSet1 =
        TestUtil.createEmptyFeatureSet("feature1", feast.core.model.Source.fromProto(source1));
    FeatureSet featureSet2 =
        TestUtil.createEmptyFeatureSet("feature2", feast.core.model.Source.fromProto(source2));

    when(featureSetRepository.findAllByNameLikeAndProject_NameLikeOrderByNameAsc("features1", "%"))
        .thenReturn(Lists.newArrayList(featureSet1));
    when(featureSetRepository.findAllByNameLikeAndProject_NameLikeOrderByNameAsc("features2", "%"))
        .thenReturn(Lists.newArrayList(featureSet2));

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
  public void shouldCreateJobIfNoRunning() {
    Source source = DataGenerator.createSource("kafka:9092", "topic");
    Store store = DataGenerator.getDefaultStore();

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

    FeatureSet featureSet =
        TestUtil.createEmptyFeatureSet("features1", feast.core.model.Source.fromProto(source));

    when(featureSetRepository.findAllByNameLikeAndProject_NameLikeOrderByNameAsc("%", "%"))
        .thenReturn(Lists.newArrayList(featureSet));
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
    ;

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
}
