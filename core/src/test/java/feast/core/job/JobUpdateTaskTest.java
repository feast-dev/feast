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
import java.util.Optional;
import org.hamcrest.core.IsNull;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class JobUpdateTaskTest {

  @Mock private JobManager jobManager;

  private StoreProto.Store store;
  private SourceProto.Source source;

  @Before
  public void setUp() {
    initMocks(this);
    store =
        StoreProto.Store.newBuilder()
            .setName("test")
            .setType(StoreType.REDIS)
            .setRedisConfig(RedisConfig.newBuilder().build())
            .addSubscriptions(Subscription.newBuilder().setName("*").setVersion(">0").build())
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
  }

  @Test
  public void shouldUpdateJobIfPresent() {
    FeatureSetSpec featureSet1 =
        FeatureSetSpec.newBuilder().setName("featureSet1").setVersion(1).setSource(source).build();
    FeatureSetSpec featureSet2 =
        FeatureSetSpec.newBuilder().setName("featureSet2").setVersion(1).setSource(source).build();
    Job originalJob =
        new Job(
            "job",
            "old_ext",
            Runner.DATAFLOW.getName(),
            feast.core.model.Source.fromProto(source),
            feast.core.model.Store.fromProto(store),
            Arrays.asList(FeatureSet.fromSpec(featureSet1)),
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
            Runner.DATAFLOW.getName(),
            feast.core.model.Source.fromProto(source),
            feast.core.model.Store.fromProto(store),
            Arrays.asList(FeatureSet.fromSpec(featureSet1), FeatureSet.fromSpec(featureSet2)),
            JobStatus.RUNNING);

    Job expected =
        new Job(
            "job",
            "new_ext",
            Runner.DATAFLOW.getName(),
            Source.fromProto(source),
            Store.fromProto(store),
            Arrays.asList(FeatureSet.fromSpec(featureSet1), FeatureSet.fromSpec(featureSet2)),
            JobStatus.PENDING);
    when(jobManager.updateJob(submittedJob)).thenReturn(expected);
    when(jobManager.getRunnerType()).thenReturn(Runner.DATAFLOW);
    Job actual = jobUpdateTask.call();

    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldCreateJobIfNotPresent() {
    FeatureSetSpec featureSet1 =
        FeatureSetSpec.newBuilder().setName("featureSet1").setVersion(1).setSource(source).build();
    JobUpdateTask jobUpdateTask =
        spy(
            new JobUpdateTask(
                Arrays.asList(featureSet1), source, store, Optional.empty(), jobManager, 100L));
    doReturn("job").when(jobUpdateTask).createJobId("KAFKA/servers:9092/topic", "test");

    Job expected =
        new Job(
            "job",
            "ext",
            Runner.DATAFLOW.getName(),
            feast.core.model.Source.fromProto(source),
            feast.core.model.Store.fromProto(store),
            Arrays.asList(FeatureSet.fromSpec(featureSet1)),
            JobStatus.RUNNING);

    when(jobManager.getRunnerType()).thenReturn(Runner.DATAFLOW);
    when(jobManager.startJob("job", Arrays.asList(featureSet1), source, store))
        .thenReturn(expected);

    Job actual = jobUpdateTask.call();
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldUpdateJobStatusIfNotCreateOrUpdate() {
    FeatureSetSpec featureSet1 =
        FeatureSetSpec.newBuilder().setName("featureSet1").setVersion(1).setSource(source).build();
    Job originalJob =
        new Job(
            "job",
            "ext",
            Runner.DATAFLOW.getName(),
            feast.core.model.Source.fromProto(source),
            feast.core.model.Store.fromProto(store),
            Arrays.asList(FeatureSet.fromSpec(featureSet1)),
            JobStatus.RUNNING);
    JobUpdateTask jobUpdateTask =
        new JobUpdateTask(
            Arrays.asList(featureSet1), source, store, Optional.of(originalJob), jobManager, 100L);

    when(jobManager.getJobStatus(originalJob)).thenReturn(JobStatus.ABORTING);
    Job expected =
        new Job(
            "job",
            "ext",
            Runner.DATAFLOW.getName(),
            Source.fromProto(source),
            Store.fromProto(store),
            Arrays.asList(FeatureSet.fromSpec(featureSet1)),
            JobStatus.ABORTING);
    Job actual = jobUpdateTask.call();

    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldReturnJobWithErrorStatusIfFailedToSubmit() {
    FeatureSetSpec featureSet1 =
        FeatureSetSpec.newBuilder().setName("featureSet1").setVersion(1).setSource(source).build();
    JobUpdateTask jobUpdateTask =
        spy(
            new JobUpdateTask(
                Arrays.asList(featureSet1), source, store, Optional.empty(), jobManager, 100L));
    doReturn("job").when(jobUpdateTask).createJobId("KAFKA/servers:9092/topic", "test");

    Job expected =
        new Job(
            "job",
            "",
            Runner.DATAFLOW.getName(),
            feast.core.model.Source.fromProto(source),
            feast.core.model.Store.fromProto(store),
            Arrays.asList(FeatureSet.fromSpec(featureSet1)),
            JobStatus.ERROR);

    when(jobManager.getRunnerType()).thenReturn(Runner.DATAFLOW);
    when(jobManager.startJob("job", Arrays.asList(featureSet1), source, store))
        .thenThrow(new RuntimeException("Something went wrong"));

    Job actual = jobUpdateTask.call();
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldTimeout() {
    FeatureSetSpec featureSet1 =
        FeatureSetSpec.newBuilder().setName("featureSet1").setVersion(1).setSource(source).build();
    JobUpdateTask jobUpdateTask =
        spy(
            new JobUpdateTask(
                Arrays.asList(featureSet1), source, store, Optional.empty(), jobManager, 0L));
    Job actual = jobUpdateTask.call();
    assertThat(actual, is(IsNull.nullValue()));
  }
}
