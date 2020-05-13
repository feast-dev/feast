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
  private static final Runner RUNNER = Runner.DATAFLOW;

  private static final FeatureSetProto.FeatureSet.Builder fsBuilder =
      FeatureSetProto.FeatureSet.newBuilder().setMeta(FeatureSetMeta.newBuilder());
  private static final FeatureSetSpec.Builder specBuilder =
      FeatureSetSpec.newBuilder().setProject("project1");

  @Mock private JobManager jobManager;

  private Store store;
  private Source source;
  private FeatureSet featureSet1;

  @Before
  public void setUp() {
    initMocks(this);
    when(jobManager.getRunnerType()).thenReturn(RUNNER);

    store =
        Store.fromProto(
            StoreProto.Store.newBuilder()
                .setName("test")
                .setType(StoreType.REDIS)
                .setRedisConfig(RedisConfig.newBuilder().build())
                .addSubscriptions(Subscription.newBuilder().setProject("*").setName("*").build())
                .build());

    source =
        Source.fromProto(
            SourceProto.Source.newBuilder()
                .setType(SourceType.KAFKA)
                .setKafkaSourceConfig(
                    KafkaSourceConfig.newBuilder()
                        .setTopic("topic")
                        .setBootstrapServers("servers:9092")
                        .build())
                .build());

    featureSet1 =
        FeatureSet.fromProto(fsBuilder.setSpec(specBuilder.setName("featureSet1")).build());
    featureSet1.setSource(source);
  }

  Job makeJob(String extId, List<FeatureSet> featureSets, JobStatus status) {
    return new Job("job", extId, RUNNER, source, store, featureSets, status);
  }

  JobUpdateTask makeTask(List<FeatureSet> featureSets, Optional<Job> currentJob) {
    return new JobUpdateTask(featureSets, source, store, currentJob, jobManager, 100L);
  }

  @Test
  public void shouldUpdateJobIfPresent() {
    FeatureSet featureSet2 =
        FeatureSet.fromProto(fsBuilder.setSpec(specBuilder.setName("featureSet2")).build());
    List<FeatureSet> existingFeatureSetsPopulatedByJob = Collections.singletonList(featureSet1);
    List<FeatureSet> newFeatureSetsPopulatedByJob = Arrays.asList(featureSet1, featureSet2);

    Job originalJob = makeJob("old_ext", existingFeatureSetsPopulatedByJob, JobStatus.RUNNING);
    JobUpdateTask jobUpdateTask = makeTask(newFeatureSetsPopulatedByJob, Optional.of(originalJob));
    Job submittedJob = makeJob("old_ext", newFeatureSetsPopulatedByJob, JobStatus.RUNNING);

    Job expected = makeJob("new_ext", newFeatureSetsPopulatedByJob, JobStatus.PENDING);
    when(jobManager.updateJob(submittedJob)).thenReturn(expected);
    Job actual = jobUpdateTask.call();

    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldCreateJobIfNotPresent() {
    var featureSets = Collections.singletonList(featureSet1);
    JobUpdateTask jobUpdateTask = spy(makeTask(featureSets, Optional.empty()));
    doReturn("job").when(jobUpdateTask).createJobId("KAFKA/servers:9092/topic", "test");

    Job expectedInput = makeJob("", featureSets, JobStatus.PENDING);
    Job expected = makeJob("ext", featureSets, JobStatus.PENDING);

    when(jobManager.startJob(expectedInput)).thenReturn(expected);

    Job actual = jobUpdateTask.call();
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldUpdateJobStatusIfNotCreateOrUpdate() {
    var featureSets = Collections.singletonList(featureSet1);
    Job originalJob = makeJob("ext", featureSets, JobStatus.RUNNING);
    JobUpdateTask jobUpdateTask = makeTask(featureSets, Optional.of(originalJob));

    when(jobManager.getJobStatus(originalJob)).thenReturn(JobStatus.ABORTING);
    Job updated = jobUpdateTask.call();

    assertThat(updated.getStatus(), equalTo(JobStatus.ABORTING));
  }

  @Test
  public void shouldReturnJobWithErrorStatusIfFailedToSubmit() {
    var featureSets = Collections.singletonList(featureSet1);
    JobUpdateTask jobUpdateTask = spy(makeTask(featureSets, Optional.empty()));
    doReturn("job").when(jobUpdateTask).createJobId("KAFKA/servers:9092/topic", "test");

    Job expectedInput = makeJob("", featureSets, JobStatus.PENDING);
    Job expected = makeJob("", featureSets, JobStatus.ERROR);

    when(jobManager.startJob(expectedInput))
        .thenThrow(new RuntimeException("Something went wrong"));

    Job actual = jobUpdateTask.call();
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldTimeout() {
    var featureSets = Collections.singletonList(featureSet1);
    var timeoutSeconds = 0L;
    JobUpdateTask jobUpdateTask =
        spy(
            new JobUpdateTask(
                featureSets, source, store, Optional.empty(), jobManager, timeoutSeconds));

    Job actual = jobUpdateTask.call();
    assertThat(actual, is(IsNull.nullValue()));
  }
}
