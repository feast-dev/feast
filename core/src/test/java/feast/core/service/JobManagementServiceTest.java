/*
 * Copyright 2018 The Feast Authors
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
 *
 */

package feast.core.service;

import com.google.common.collect.Lists;
import com.google.protobuf.Timestamp;
import feast.core.JobServiceProto.JobServiceTypes.JobDetail;
import feast.core.dao.JobInfoRepository;
import feast.core.dao.MetricsRepository;
import feast.core.exception.RetrievalException;
import feast.core.job.JobManager;
import feast.core.model.JobInfo;
import feast.core.model.JobStatus;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class JobManagementServiceTest {
  @Mock private JobInfoRepository jobInfoRepository;
  @Mock private MetricsRepository metricsRepository;
  @Mock private JobManager jobManager;

  @Rule public final ExpectedException exception = ExpectedException.none();

  @Before
  public void setUp() {
    initMocks(this);
  }

  @Test
  public void shouldListAllJobDetails() {
    JobInfo jobInfo1 =
        new JobInfo(
            "job1",
            "",
            "",
            "",
            "",
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            JobStatus.PENDING,
            "");
    jobInfo1.setCreated(Date.from(Instant.ofEpochSecond(1)));
    jobInfo1.setLastUpdated(Date.from(Instant.ofEpochSecond(1)));
    JobInfo jobInfo2 =
        new JobInfo(
            "job2",
            "",
            "",
            "",
            "",
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            JobStatus.PENDING,
            "");
    jobInfo2.setCreated(Date.from(Instant.ofEpochSecond(1)));
    jobInfo2.setLastUpdated(Date.from(Instant.ofEpochSecond(1)));
    when(jobInfoRepository.findAll()).thenReturn(Lists.newArrayList(jobInfo1, jobInfo2));
    JobManagementService jobManagementService =
        new JobManagementService(jobInfoRepository, metricsRepository, jobManager);
    List<JobDetail> actual = jobManagementService.listJobs();
    List<JobDetail> expected =
        Lists.newArrayList(
            JobDetail.newBuilder()
                .setId("job1")
                .setStatus("PENDING")
                .setCreated(Timestamp.newBuilder().setSeconds(1).build())
                .setLastUpdated(Timestamp.newBuilder().setSeconds(1).build())
                .build(),
            JobDetail.newBuilder()
                .setId("job2")
                .setStatus("PENDING")
                .setCreated(Timestamp.newBuilder().setSeconds(1).build())
                .setLastUpdated(Timestamp.newBuilder().setSeconds(1).build())
                .build());
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldReturnDetailOfRequestedJobId() {
    JobInfo jobInfo1 =
        new JobInfo(
            "job1",
            "",
            "",
            "",
            "",
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            JobStatus.PENDING,
            "");
    jobInfo1.setCreated(Date.from(Instant.ofEpochSecond(1)));
    jobInfo1.setLastUpdated(Date.from(Instant.ofEpochSecond(1)));
    when(jobInfoRepository.findById("job1")).thenReturn(Optional.of(jobInfo1));
    JobManagementService jobManagementService =
        new JobManagementService(jobInfoRepository, metricsRepository, jobManager);
    JobDetail actual = jobManagementService.getJob("job1");
    JobDetail expected =
        JobDetail.newBuilder()
            .setId("job1")
            .setStatus("PENDING")
            .setCreated(Timestamp.newBuilder().setSeconds(1).build())
            .setLastUpdated(Timestamp.newBuilder().setSeconds(1).build())
            .build();
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldThrowErrorIfJobIdNotFoundWhenGettingJob() {
    when(jobInfoRepository.findById("job1")).thenReturn(Optional.empty());
    JobManagementService jobManagementService =
        new JobManagementService(jobInfoRepository, metricsRepository, jobManager);
    exception.expect(RetrievalException.class);
    exception.expectMessage("Unable to retrieve job with id job1");
    jobManagementService.getJob("job1");
  }

  @Test
  public void shouldThrowErrorIfJobIdNotFoundWhenAbortingJob() {
    when(jobInfoRepository.findById("job1")).thenReturn(Optional.empty());
    JobManagementService jobManagementService =
        new JobManagementService(jobInfoRepository, metricsRepository, jobManager);
    exception.expect(RetrievalException.class);
    exception.expectMessage("Unable to retrieve job with id job1");
    jobManagementService.abortJob("job1");
  }

  @Test
  public void shouldThrowErrorIfJobInTerminalStateWhenAbortingJob() {
    JobInfo job = new JobInfo();
    job.setStatus(JobStatus.COMPLETED);
    when(jobInfoRepository.findById("job1")).thenReturn(Optional.of(job));
    JobManagementService jobManagementService =
        new JobManagementService(jobInfoRepository, metricsRepository, jobManager);
    exception.expect(IllegalStateException.class);
    exception.expectMessage("Unable to stop job already in terminal state");
    jobManagementService.abortJob("job1");
  }

  @Test
  public void shouldUpdateJobAfterAborting() {
    JobInfo job = new JobInfo();
    job.setStatus(JobStatus.RUNNING);
    job.setExtId("extId1");
    when(jobInfoRepository.findById("job1")).thenReturn(Optional.of(job));
    JobManagementService jobManagementService =
        new JobManagementService(jobInfoRepository, metricsRepository, jobManager);
    jobManagementService.abortJob("job1");
    ArgumentCaptor<JobInfo> jobCapture = ArgumentCaptor.forClass(JobInfo.class);
    verify(jobInfoRepository).saveAndFlush(jobCapture.capture());
    assertThat(jobCapture.getValue().getStatus(), equalTo(JobStatus.ABORTING));
  }
}
