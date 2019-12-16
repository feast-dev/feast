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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.CoreServiceProto.ListFeatureSetsRequest.Filter;
import feast.core.CoreServiceProto.ListFeatureSetsResponse;
import feast.core.CoreServiceProto.ListStoresResponse;
import feast.core.FeatureSetProto;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.SourceProto.KafkaSourceConfig;
import feast.core.SourceProto.Source;
import feast.core.SourceProto.SourceType;
import feast.core.StoreProto;
import feast.core.StoreProto.Store.RedisConfig;
import feast.core.StoreProto.Store.StoreType;
import feast.core.StoreProto.Store.Subscription;
import feast.core.dao.FeatureSetRepository;
import feast.core.dao.JobInfoRepository;
import feast.core.job.JobManager;
import feast.core.job.Runner;
import feast.core.model.FeatureSet;
import feast.core.model.JobInfo;
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
  @Mock JobInfoRepository jobInfoRepository;
  @Mock JobManager jobManager;
  @Mock SpecService specService;
  @Mock FeatureSetRepository featureSetRepository;

  @Before
  public void setUp() {
    initMocks(this);
  }

  @Test
  public void shouldDoNothingIfNoStoresFound() {
    when(specService.listStores(any())).thenReturn(ListStoresResponse.newBuilder().build());
    JobCoordinatorService jcs =
        new JobCoordinatorService(jobInfoRepository, featureSetRepository, specService, jobManager);
    jcs.Poll();
    verify(jobInfoRepository, times(0)).saveAndFlush(any());
  }

  @Test
  public void shouldDoNothingIfNoMatchingFeatureSetsFound() throws InvalidProtocolBufferException {
    StoreProto.Store store =
        StoreProto.Store.newBuilder()
            .setName("test")
            .setType(StoreType.REDIS)
            .setRedisConfig(RedisConfig.newBuilder().build())
            .addSubscriptions(Subscription.newBuilder().setName("*").setVersion(">0").build())
            .build();
    when(specService.listStores(any()))
        .thenReturn(ListStoresResponse.newBuilder().addStore(store).build());
    when(specService.listFeatureSets(
            Filter.newBuilder().setFeatureSetName("*").setFeatureSetVersion(">0").build()))
        .thenReturn(ListFeatureSetsResponse.newBuilder().build());
    JobCoordinatorService jcs =
        new JobCoordinatorService(jobInfoRepository, featureSetRepository, specService, jobManager);
    jcs.Poll();
    verify(jobInfoRepository, times(0)).saveAndFlush(any());
  }

  @Test
  public void shouldGenerateAndSubmitJobsIfAny() throws InvalidProtocolBufferException {
    StoreProto.Store store =
        StoreProto.Store.newBuilder()
            .setName("test")
            .setType(StoreType.REDIS)
            .setRedisConfig(RedisConfig.newBuilder().build())
            .addSubscriptions(
                Subscription.newBuilder().setName("features").setVersion(">0").build())
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

    FeatureSetSpec featureSet1 =
        FeatureSetSpec.newBuilder().setName("features").setVersion(1).setSource(source).build();
    FeatureSetSpec featureSet2 =
        FeatureSetSpec.newBuilder().setName("features").setVersion(2).setSource(source).build();
    String extId = "ext";
    ArgumentCaptor<JobInfo> jobInfoArgCaptor = ArgumentCaptor.forClass(JobInfo.class);

    when(specService.listFeatureSets(
            Filter.newBuilder().setFeatureSetName("features").setFeatureSetVersion(">0").build()))
        .thenReturn(
            ListFeatureSetsResponse.newBuilder()
                .addFeatureSets(FeatureSetProto.FeatureSet.newBuilder().setSpec(featureSet1))
                .addFeatureSets(FeatureSetProto.FeatureSet.newBuilder().setSpec(featureSet2))
                .build());
    when(specService.listStores(any()))
        .thenReturn(ListStoresResponse.newBuilder().addStore(store).build());

    when(jobManager.startJob(any(), eq(Arrays.asList(featureSet1, featureSet2)), eq(store)))
        .thenReturn(extId);
    when(jobManager.getRunnerType()).thenReturn(Runner.DATAFLOW);

    JobCoordinatorService jcs =
        new JobCoordinatorService(jobInfoRepository, featureSetRepository, specService, jobManager);
    jcs.Poll();
    verify(jobInfoRepository, times(1)).saveAndFlush(jobInfoArgCaptor.capture());
    JobInfo actual = jobInfoArgCaptor.getValue();
    JobInfo expected =
        new JobInfo(
            actual.getId(),
            extId,
            Runner.DATAFLOW.getName(),
            feast.core.model.Source.fromProto(source),
            feast.core.model.Store.fromProto(store),
            Arrays.asList(FeatureSet.fromSpec(featureSet1), FeatureSet.fromSpec(featureSet2)),
            JobStatus.RUNNING);
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
                Subscription.newBuilder().setName("features").setVersion(">0").build())
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

    FeatureSetSpec featureSet1 =
        FeatureSetSpec.newBuilder().setName("features").setVersion(1).setSource(source1).build();
    FeatureSetSpec featureSet2 =
        FeatureSetSpec.newBuilder().setName("features").setVersion(2).setSource(source2).build();
    String extId1 = "ext1";
    String extId2 = "ext2";
    ArgumentCaptor<JobInfo> jobInfoArgCaptor = ArgumentCaptor.forClass(JobInfo.class);

    when(specService.listFeatureSets(
            Filter.newBuilder().setFeatureSetName("features").setFeatureSetVersion(">0").build()))
        .thenReturn(
            ListFeatureSetsResponse.newBuilder()
                .addFeatureSets(FeatureSetProto.FeatureSet.newBuilder().setSpec(featureSet1))
                .addFeatureSets(FeatureSetProto.FeatureSet.newBuilder().setSpec(featureSet2))
                .build());
    when(specService.listStores(any()))
        .thenReturn(ListStoresResponse.newBuilder().addStore(store).build());

    when(jobManager.startJob(any(), eq(Arrays.asList(featureSet1)), eq(store))).thenReturn(extId1);
    when(jobManager.startJob(any(), eq(Arrays.asList(featureSet2)), eq(store))).thenReturn(extId2);
    when(jobManager.getRunnerType()).thenReturn(Runner.DATAFLOW);

    JobCoordinatorService jcs =
        new JobCoordinatorService(jobInfoRepository, featureSetRepository, specService, jobManager);
    jcs.Poll();

    verify(jobInfoRepository, times(2)).saveAndFlush(jobInfoArgCaptor.capture());
    List<JobInfo> actual = jobInfoArgCaptor.getAllValues();
    JobInfo expected1 =
        new JobInfo(
            actual.get(0).getId(),
            extId1,
            Runner.DATAFLOW.getName(),
            feast.core.model.Source.fromProto(source1),
            feast.core.model.Store.fromProto(store),
            Arrays.asList(FeatureSet.fromSpec(featureSet1)),
            JobStatus.RUNNING);
    assertThat(actual.get(0), equalTo(expected1));

    JobInfo expected2 =
        new JobInfo(
            actual.get(1).getId(),
            extId2,
            Runner.DATAFLOW.getName(),
            feast.core.model.Source.fromProto(source2),
            feast.core.model.Store.fromProto(store),
            Arrays.asList(FeatureSet.fromSpec(featureSet2)),
            JobStatus.RUNNING);
    assertThat(actual.get(1), equalTo(expected2));
  }
}
