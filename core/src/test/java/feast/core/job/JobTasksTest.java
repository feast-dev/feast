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

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.collect.ImmutableSet;
import feast.core.model.*;
import feast.core.util.TestUtil;
import feast.proto.core.SourceProto;
import feast.proto.core.SourceProto.KafkaSourceConfig;
import feast.proto.core.SourceProto.SourceType;
import feast.proto.core.StoreProto;
import feast.proto.core.StoreProto.Store.RedisConfig;
import feast.proto.core.StoreProto.Store.StoreType;
import feast.proto.core.StoreProto.Store.Subscription;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class JobTasksTest {
  private static final Runner RUNNER = Runner.DATAFLOW;

  @Mock private JobManager jobManager;

  private Store store;
  private Source source;
  private FeatureSet featureSet1;
  private FeatureSet featureSet2;

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
  }

  Job makeJob(String extId, List<FeatureSet> featureSets, JobStatus status) {
    Job job =
        Job.builder()
            .setId("job")
            .setExtId(extId)
            .setRunner(RUNNER)
            .setSource(source)
            .setFeatureSetJobStatuses(TestUtil.makeFeatureSetJobStatus(featureSets))
            .setStatus(status)
            .build();
    job.setStores(ImmutableSet.of(store));
    return job;
  }

  CreateJobTask makeCreateTask(Job currentJob) {
    return CreateJobTask.builder().setJob(currentJob).setJobManager(jobManager).build();
  }

  UpgradeJobTask makeUpgradeTask(Job currentJob) {
    return UpgradeJobTask.builder().setJob(currentJob).setJobManager(jobManager).build();
  }

  UpdateJobStatusTask makeCheckStatusTask(Job currentJob) {
    return UpdateJobStatusTask.builder().setJob(currentJob).setJobManager(jobManager).build();
  }

  TerminateJobTask makeTerminateTask(Job currentJob) {
    return TerminateJobTask.builder().setJob(currentJob).setJobManager(jobManager).build();
  }

  @Test
  public void shouldCreateJobIfNotPresent() {
    Job expectedInput = makeJob("ext", Collections.emptyList(), JobStatus.PENDING);

    CreateJobTask task = makeCreateTask(expectedInput);

    Job expected = makeJob("ext", Collections.emptyList(), JobStatus.RUNNING);

    when(jobManager.startJob(expectedInput)).thenReturn(expected);

    Job actual = task.call();
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldUpdateJobStatusIfNotCreateOrUpdate() {
    Job originalJob = makeJob("ext", Collections.emptyList(), JobStatus.RUNNING);
    JobTask jobUpdateTask = makeCheckStatusTask(originalJob);

    when(jobManager.getJobStatus(originalJob)).thenReturn(JobStatus.ABORTING);
    Job updated = jobUpdateTask.call();

    assertThat(updated.getStatus(), equalTo(JobStatus.ABORTING));
  }

  @Test
  public void shouldReturnJobWithErrorStatusIfFailedToSubmit() {
    Job expectedInput = makeJob("", Collections.emptyList(), JobStatus.PENDING);

    CreateJobTask jobUpdateTask = makeCreateTask(expectedInput);

    Job expected = makeJob("", Collections.emptyList(), JobStatus.ERROR);

    when(jobManager.startJob(expectedInput))
        .thenThrow(new RuntimeException("Something went wrong"));

    Job actual = jobUpdateTask.call();
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldStopJobIfTargetStatusIsAbort() {
    Job originalJob = makeJob("ext", Collections.emptyList(), JobStatus.RUNNING);
    JobTask jobUpdateTask = makeTerminateTask(originalJob);

    Job expected = makeJob("ext", Collections.emptyList(), JobStatus.ABORTING);

    when(jobManager.getJobStatus(originalJob)).thenReturn(JobStatus.ABORTING);
    when(jobManager.abortJob(originalJob)).thenReturn(expected);

    Job actual = jobUpdateTask.call();
    verify(jobManager, times(1)).abortJob(originalJob);
    assertThat(actual, equalTo(expected));
  }
}
