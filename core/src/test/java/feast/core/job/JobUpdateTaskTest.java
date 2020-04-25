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
package feast.core.job;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import feast.core.FeatureSetProto;
import feast.core.FeatureSetProto.FeatureSetMeta;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.SourceProto;
import feast.core.SourceProto.KafkaSourceConfig;
import feast.core.SourceProto.SourceType;
import feast.core.StoreProto;
import feast.core.StoreProto.Store.RedisConfig;
import feast.core.StoreProto.Store.StoreType;
import feast.core.StoreProto.Store.Subscription;
import feast.core.model.FeatureSet;
import feast.core.model.Job;
import feast.core.model.JobStatus;
import feast.core.model.Source;
import feast.core.model.Store;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.hamcrest.core.IsNull;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class JobUpdateTaskTest {

  private static final FeatureSetProto.FeatureSet.Builder fsBuilder =
      FeatureSetProto.FeatureSet.newBuilder().setMeta(FeatureSetMeta.newBuilder());
  private static final FeatureSetSpec.Builder specBuilder =
      FeatureSetSpec.newBuilder().setProject("project1").setVersion(1);

  @Mock private JobManager jobManager;

  private StoreProto.Store store;
  private SourceProto.Source source;
  private FeatureSetProto.FeatureSet featureSet1;

  @Before
  public void setUp() {
    initMocks(this);
    when(jobManager.getRunnerType()).thenReturn(Runner.DATAFLOW);

    store =
        StoreProto.Store.newBuilder()
            .setName("test")
            .setType(StoreType.REDIS)
            .setRedisConfig(RedisConfig.newBuilder().build())
            .addSubscriptions(
                Subscription.newBuilder().setProject("*").setName("*").setVersion("*").build())
            .build();

    source =
        SourceProto.Source.newBuilder()
            .setType(SourceType.KAFKA)
            .setKafkaSourceConfig(
                KafkaSourceConfig.newBuilder()
                    .setTopic("topic")
                    .setBootstrapServers("servers:9092")
                    .build())
            .build();

    featureSet1 = fsBuilder.setSpec(specBuilder.setName("featureSet1").setSource(source)).build();
  }

  @Test
  public void shouldUpdateJobIfPresent() {
    FeatureSetProto.FeatureSet featureSet2 =
        fsBuilder.setSpec(specBuilder.setName("featureSet2")).build();
    List<FeatureSet> existingFeatureSetsPopulatedByJob =
        Collections.singletonList(FeatureSet.fromProto(featureSet1));
    List<FeatureSet> newFeatureSetsPopulatedByJob =
        Arrays.asList(FeatureSet.fromProto(featureSet1), FeatureSet.fromProto(featureSet2));

    Job originalJob =
        new Job(
            "job",
            "old_ext",
            Runner.DATAFLOW,
            Source.fromProto(source),
            Store.fromProto(store),
            existingFeatureSetsPopulatedByJob,
            JobStatus.RUNNING);
    JobUpdateTask jobUpdateTask =
        new JobUpdateTask(
            Arrays.asList(featureSet1, featureSet2),
            source,
            store,
            Optional.of(originalJob),
            jobManager,
            100L);
    Job submittedJob =
        new Job(
            "job",
            "old_ext",
            Runner.DATAFLOW,
            Source.fromProto(source),
            Store.fromProto(store),
            newFeatureSetsPopulatedByJob,
            JobStatus.RUNNING);

    Job expected =
        new Job(
            "job",
            "new_ext",
            Runner.DATAFLOW,
            Source.fromProto(source),
            Store.fromProto(store),
            newFeatureSetsPopulatedByJob,
            JobStatus.PENDING);
    when(jobManager.updateJob(submittedJob)).thenReturn(expected);
    Job actual = jobUpdateTask.call();

    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldCreateJobIfNotPresent() {
    JobUpdateTask jobUpdateTask =
        spy(
            new JobUpdateTask(
                Arrays.asList(featureSet1), source, store, Optional.empty(), jobManager, 100L));
    doReturn("job").when(jobUpdateTask).createJobId("KAFKA/servers:9092/topic", "test");

    Job expectedInput =
        new Job(
            "job",
            "",
            Runner.DATAFLOW,
            Source.fromProto(source),
            Store.fromProto(store),
            Arrays.asList(FeatureSet.fromProto(featureSet1)),
            JobStatus.PENDING);

    Job expected =
        new Job(
            "job",
            "ext",
            Runner.DATAFLOW,
            Source.fromProto(source),
            Store.fromProto(store),
            Arrays.asList(FeatureSet.fromProto(featureSet1)),
            JobStatus.RUNNING);

    when(jobManager.startJob(expectedInput)).thenReturn(expected);

    Job actual = jobUpdateTask.call();
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldUpdateJobStatusIfNotCreateOrUpdate() {
    Job originalJob =
        new Job(
            "job",
            "ext",
            Runner.DATAFLOW,
            Source.fromProto(source),
            Store.fromProto(store),
            Arrays.asList(FeatureSet.fromProto(featureSet1)),
            JobStatus.RUNNING);
    JobUpdateTask jobUpdateTask =
        new JobUpdateTask(
            Arrays.asList(featureSet1), source, store, Optional.of(originalJob), jobManager, 100L);

    when(jobManager.getJobStatus(originalJob)).thenReturn(JobStatus.ABORTING);
    Job updated = jobUpdateTask.call();

    assertThat(updated.getStatus(), equalTo(JobStatus.ABORTING));
  }

  @Test
  public void shouldReturnJobWithErrorStatusIfFailedToSubmit() {
    JobUpdateTask jobUpdateTask =
        spy(
            new JobUpdateTask(
                Arrays.asList(featureSet1), source, store, Optional.empty(), jobManager, 100L));
    doReturn("job").when(jobUpdateTask).createJobId("KAFKA/servers:9092/topic", "test");

    Job expectedInput =
        new Job(
            "job",
            "",
            Runner.DATAFLOW,
            Source.fromProto(source),
            Store.fromProto(store),
            Arrays.asList(FeatureSet.fromProto(featureSet1)),
            JobStatus.PENDING);

    Job expected =
        new Job(
            "job",
            "",
            Runner.DATAFLOW,
            Source.fromProto(source),
            Store.fromProto(store),
            Arrays.asList(FeatureSet.fromProto(featureSet1)),
            JobStatus.ERROR);

    when(jobManager.startJob(expectedInput))
        .thenThrow(new RuntimeException("Something went wrong"));

    Job actual = jobUpdateTask.call();
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldTimeout() {
    JobUpdateTask jobUpdateTask =
        spy(
            new JobUpdateTask(
                Arrays.asList(featureSet1), source, store, Optional.empty(), jobManager, 0L));
    Job actual = jobUpdateTask.call();
    assertThat(actual, is(IsNull.nullValue()));
  }
}
