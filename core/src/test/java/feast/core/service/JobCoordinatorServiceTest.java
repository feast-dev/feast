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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.CoreServiceProto.ListFeatureSetsRequest.Filter;
import feast.core.CoreServiceProto.ListFeatureSetsResponse;
import feast.core.CoreServiceProto.ListStoresResponse;
import feast.core.FeatureSetProto;
import feast.core.FeatureSetProto.FeatureSetMeta;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.SourceProto.KafkaSourceConfig;
import feast.core.SourceProto.Source;
import feast.core.SourceProto.SourceType;
import feast.core.StoreProto;
import feast.core.StoreProto.Store.RedisConfig;
import feast.core.StoreProto.Store.StoreType;
import feast.core.StoreProto.Store.Subscription;
import feast.core.config.FeastProperties;
import feast.core.config.FeastProperties.JobProperties;
import feast.core.dao.FeatureSetRepository;
import feast.core.dao.JobRepository;
import feast.core.job.JobManager;
import feast.core.job.JobMatcher;
import feast.core.job.Runner;
import feast.core.model.FeatureSet;
import feast.core.model.Job;
import feast.core.model.JobStatus;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

public class JobCoordinatorServiceTest {

  @Rule public final ExpectedException exception = ExpectedException.none();
  @Mock JobRepository jobRepository;
  @Mock JobManager jobManager;
  @Mock SpecService specService;
  @Mock FeatureSetRepository featureSetRepository;

  private FeastProperties feastProperties;

  @Before
  public void setUp() {
    initMocks(this);
    feastProperties = new FeastProperties();
    JobProperties jobProperties = new JobProperties();
    jobProperties.setJobUpdateTimeoutSeconds(5);
    feastProperties.setJobs(jobProperties);
  }

  @Test
  public void shouldDoNothingIfNoStoresFound() throws InvalidProtocolBufferException {
    when(specService.listStores(any())).thenReturn(ListStoresResponse.newBuilder().build());
    JobCoordinatorService jcs =
        new JobCoordinatorService(
            jobRepository, featureSetRepository, specService, jobManager, feastProperties);
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
            .addSubscriptions(
                Subscription.newBuilder().setProject("*").setName("*").setVersion("*").build())
            .build();
    when(specService.listStores(any()))
        .thenReturn(ListStoresResponse.newBuilder().addStore(store).build());
    when(specService.listFeatureSets(
            Filter.newBuilder()
                .setProject("*")
                .setFeatureSetName("*")
                .setFeatureSetVersion("*")
                .build()))
        .thenReturn(ListFeatureSetsResponse.newBuilder().build());
    JobCoordinatorService jcs =
        new JobCoordinatorService(
            jobRepository, featureSetRepository, specService, jobManager, feastProperties);
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
            .addSubscriptions(
                Subscription.newBuilder()
                    .setProject("project1")
                    .setName("features")
                    .setVersion("*")
                    .build())
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

    FeatureSetProto.FeatureSet featureSet1 =
        FeatureSetProto.FeatureSet.newBuilder()
            .setSpec(
                FeatureSetSpec.newBuilder()
                    .setSource(source)
                    .setProject("project1")
                    .setName("features")
                    .setVersion(1))
            .setMeta(FeatureSetMeta.newBuilder())
            .build();
    FeatureSetProto.FeatureSet featureSet2 =
        FeatureSetProto.FeatureSet.newBuilder()
            .setSpec(
                FeatureSetSpec.newBuilder()
                    .setSource(source)
                    .setProject("project1")
                    .setName("features")
                    .setVersion(2))
            .setMeta(FeatureSetMeta.newBuilder())
            .build();
    String extId = "ext";
    ArgumentCaptor<Job> jobArgCaptor = ArgumentCaptor.forClass(Job.class);

    Job expectedInput =
        new Job(
            "",
            "",
            Runner.DATAFLOW.name(),
            feast.core.model.Source.fromProto(source),
            feast.core.model.Store.fromProto(store),
            Arrays.asList(FeatureSet.fromProto(featureSet1), FeatureSet.fromProto(featureSet2)),
            JobStatus.PENDING);

    Job expected =
        new Job(
            "some_id",
            extId,
            Runner.DATAFLOW.name(),
            feast.core.model.Source.fromProto(source),
            feast.core.model.Store.fromProto(store),
            Arrays.asList(FeatureSet.fromProto(featureSet1), FeatureSet.fromProto(featureSet2)),
            JobStatus.RUNNING);

    when(specService.listFeatureSets(
            Filter.newBuilder()
                .setProject("project1")
                .setFeatureSetName("features")
                .setFeatureSetVersion("*")
                .build()))
        .thenReturn(
            ListFeatureSetsResponse.newBuilder()
                .addFeatureSets(featureSet1)
                .addFeatureSets(featureSet2)
                .build());
    when(specService.listStores(any()))
        .thenReturn(ListStoresResponse.newBuilder().addStore(store).build());

    when(jobManager.startJob(argThat(new JobMatcher(expectedInput)))).thenReturn(expected);
    when(jobManager.getRunnerType()).thenReturn(Runner.DATAFLOW);

    JobCoordinatorService jcs =
        new JobCoordinatorService(
            jobRepository, featureSetRepository, specService, jobManager, feastProperties);
    jcs.Poll();
    verify(jobRepository, times(1)).saveAndFlush(jobArgCaptor.capture());
    Job actual = jobArgCaptor.getValue();
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldGroupJobsBySource() throws InvalidProtocolBufferException {
    StoreProto.Store store =
        StoreProto.Store.newBuilder()
            .setName("test")
            .setType(StoreType.REDIS)
            .setRedisConfig(RedisConfig.newBuilder().build())
            .addSubscriptions(
                Subscription.newBuilder()
                    .setProject("project1")
                    .setName("features")
                    .setVersion("*")
                    .build())
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

    FeatureSetProto.FeatureSet featureSet1 =
        FeatureSetProto.FeatureSet.newBuilder()
            .setSpec(
                FeatureSetSpec.newBuilder()
                    .setSource(source1)
                    .setProject("project1")
                    .setName("features")
                    .setVersion(1))
            .setMeta(FeatureSetMeta.newBuilder())
            .build();
    FeatureSetProto.FeatureSet featureSet2 =
        FeatureSetProto.FeatureSet.newBuilder()
            .setSpec(
                FeatureSetSpec.newBuilder()
                    .setSource(source2)
                    .setProject("project1")
                    .setName("features")
                    .setVersion(2))
            .setMeta(FeatureSetMeta.newBuilder())
            .build();

    Job expectedInput1 =
        new Job(
            "name1",
            "",
            Runner.DATAFLOW.name(),
            feast.core.model.Source.fromProto(source1),
            feast.core.model.Store.fromProto(store),
            Arrays.asList(FeatureSet.fromProto(featureSet1)),
            JobStatus.PENDING);

    Job expected1 =
        new Job(
            "name1",
            "extId1",
            Runner.DATAFLOW.name(),
            feast.core.model.Source.fromProto(source1),
            feast.core.model.Store.fromProto(store),
            Arrays.asList(FeatureSet.fromProto(featureSet1)),
            JobStatus.RUNNING);

    Job expectedInput2 =
        new Job(
            "",
            "extId2",
            Runner.DATAFLOW.name(),
            feast.core.model.Source.fromProto(source2),
            feast.core.model.Store.fromProto(store),
            Arrays.asList(FeatureSet.fromProto(featureSet2)),
            JobStatus.PENDING);

    Job expected2 =
        new Job(
            "name2",
            "extId2",
            Runner.DATAFLOW.name(),
            feast.core.model.Source.fromProto(source2),
            feast.core.model.Store.fromProto(store),
            Arrays.asList(FeatureSet.fromProto(featureSet2)),
            JobStatus.RUNNING);
    ArgumentCaptor<Job> jobArgCaptor = ArgumentCaptor.forClass(Job.class);

    when(specService.listFeatureSets(
            Filter.newBuilder()
                .setProject("project1")
                .setFeatureSetName("features")
                .setFeatureSetVersion("*")
                .build()))
        .thenReturn(
            ListFeatureSetsResponse.newBuilder()
                .addFeatureSets(featureSet1)
                .addFeatureSets(featureSet2)
                .build());
    when(specService.listStores(any()))
        .thenReturn(ListStoresResponse.newBuilder().addStore(store).build());

    when(jobManager.startJob(argThat(new JobMatcher(expectedInput1)))).thenReturn(expected1);
    when(jobManager.startJob(argThat(new JobMatcher(expectedInput2)))).thenReturn(expected2);
    when(jobManager.getRunnerType()).thenReturn(Runner.DATAFLOW);

    JobCoordinatorService jcs =
        new JobCoordinatorService(
            jobRepository, featureSetRepository, specService, jobManager, feastProperties);
    jcs.Poll();

    verify(jobRepository, times(2)).saveAndFlush(jobArgCaptor.capture());
    List<Job> actual = jobArgCaptor.getAllValues();

    assertThat(actual.get(0), equalTo(expected1));
    assertThat(actual.get(1), equalTo(expected2));
  }
}
