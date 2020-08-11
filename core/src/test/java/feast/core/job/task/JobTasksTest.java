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
package feast.core.job.task;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.beans.HasPropertyWithValue.hasProperty;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.collect.ImmutableMap;
import feast.core.job.*;
import feast.core.model.Job;
import feast.core.model.JobStatus;
import feast.core.util.TestUtil;
import feast.proto.core.SourceProto;
import feast.proto.core.SourceProto.KafkaSourceConfig;
import feast.proto.core.SourceProto.SourceType;
import feast.proto.core.StoreProto;
import feast.proto.core.StoreProto.Store.RedisConfig;
import feast.proto.core.StoreProto.Store.StoreType;
import feast.proto.core.StoreProto.Store.Subscription;
import lombok.SneakyThrows;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class JobTasksTest {
  private static final Runner RUNNER = Runner.DATAFLOW;

  @Mock private JobManager jobManager;

  private StoreProto.Store store;
  private SourceProto.Source source;

  @Before
  public void setUp() {
    initMocks(this);
    when(jobManager.getRunnerType()).thenReturn(RUNNER);

    store =
        StoreProto.Store.newBuilder()
            .setName("test")
            .setType(StoreType.REDIS)
            .setRedisConfig(RedisConfig.newBuilder().build())
            .addSubscriptions(Subscription.newBuilder().setProject("*").setName("*").build())
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
    TestUtil.setupAuditLogger();
  }

  @SneakyThrows
  Job makeJob(String extId, JobStatus status) {
    Job job =
        Job.builder()
            .setId("job")
            .setSource(source)
            .setStores(ImmutableMap.of(store.getName(), store))
            .build();
    job.setStatus(status);
    job.setExtId(extId);
    return job;
  }

  CreateJobTask makeCreateTask(Job currentJob) {
    return new CreateJobTask(currentJob, jobManager);
  }

  UpdateJobStatusTask makeCheckStatusTask(Job currentJob) {
    return new UpdateJobStatusTask(currentJob, jobManager);
  }

  TerminateJobTask makeTerminateTask(Job currentJob) {
    return new TerminateJobTask(currentJob, jobManager);
  }

  @Test
  public void shouldCreateJobIfNotPresent() {
    Job expectedInput = makeJob("ext", JobStatus.PENDING);

    CreateJobTask task = makeCreateTask(expectedInput);

    when(jobManager.startJob(expectedInput)).thenReturn(makeJob("ext", JobStatus.RUNNING));

    Job actual = task.call();
    assertThat(actual, hasProperty("status", equalTo(JobStatus.RUNNING)));
  }

  @Test
  public void shouldUpdateJobStatusIfNotCreateOrUpdate() {
    Job originalJob = makeJob("ext", JobStatus.RUNNING);
    JobTask jobUpdateTask = makeCheckStatusTask(originalJob);

    when(jobManager.getJobStatus(originalJob)).thenReturn(JobStatus.ABORTING);
    Job updated = jobUpdateTask.call();

    assertThat(updated.getStatus(), equalTo(JobStatus.ABORTING));
  }

  @Test
  public void shouldReturnJobWithErrorStatusIfFailedToSubmit() {
    Job expectedInput = makeJob("", JobStatus.PENDING);

    CreateJobTask jobUpdateTask = makeCreateTask(expectedInput);

    Job expected = makeJob("", JobStatus.ERROR);

    when(jobManager.startJob(expectedInput))
        .thenThrow(new RuntimeException("Something went wrong"));

    Job actual = jobUpdateTask.call();
    assertThat(actual, hasProperty("status", equalTo(JobStatus.ERROR)));
  }

  @Test
  public void shouldStopJobIfTargetStatusIsAbort() {
    Job originalJob = makeJob("ext", JobStatus.RUNNING);
    JobTask jobUpdateTask = makeTerminateTask(originalJob);

    Job expected = makeJob("ext", JobStatus.ABORTING);

    when(jobManager.getJobStatus(originalJob)).thenReturn(JobStatus.ABORTING);
    when(jobManager.abortJob(originalJob)).thenReturn(expected);

    Job actual = jobUpdateTask.call();
    verify(jobManager, times(1)).abortJob(originalJob);
    assertThat(actual, hasProperty("status", equalTo(JobStatus.ABORTING)));
  }
}
