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

package feast.core.job;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import feast.core.dao.JobInfoRepository;
import feast.core.dao.MetricsRepository;
import feast.core.model.JobInfo;
import feast.core.model.JobStatus;
import feast.core.model.Metrics;
import feast.core.model.Store;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ScheduledJobMonitorTest {

  ScheduledJobMonitor scheduledJobMonitor;

  @Mock JobMonitor jobMonitor;

  @Mock StatsdMetricPusher stasdMetricPusher;

  @Mock JobInfoRepository jobInfoRepository;

  @Mock MetricsRepository metricsRepository;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    scheduledJobMonitor = new ScheduledJobMonitor(jobMonitor, jobInfoRepository, stasdMetricPusher);
  }

  @Test
  public void getJobStatus_shouldUpdateJobInfoForRunningJob() {
    JobInfo job =
        new JobInfo(
            "jobId",
            "extId1",
            "Streaming",
            "DataflowRunner",
            new Store(),
            Collections.emptyList(),
            Collections.emptyList(),
            JobStatus.RUNNING);

    when(jobInfoRepository.findByStatusNotIn((Collection<JobStatus>) any(Collection.class)))
        .thenReturn(Collections.singletonList(job));
    when(jobMonitor.getJobStatus(job)).thenReturn(JobStatus.COMPLETED);

    scheduledJobMonitor.getJobStatus();

    ArgumentCaptor<JobInfo> argCaptor = ArgumentCaptor.forClass(JobInfo.class);
    verify(jobInfoRepository).save(argCaptor.capture());

    JobInfo jobInfos = argCaptor.getValue();
    assertThat(jobInfos.getStatus(), equalTo(JobStatus.COMPLETED));
  }

  @Test
  public void getJobStatus_shouldNotUpdateJobInfoForTerminalJob() {
    when(jobInfoRepository.findByStatusNotIn((Collection<JobStatus>) any(Collection.class)))
        .thenReturn(Collections.emptyList());

    scheduledJobMonitor.getJobStatus();

    verify(jobInfoRepository, never()).save(any(JobInfo.class));
  }

  @Test
  public void getJobMetrics_shouldPushToStatsDMetricPusherAndSaveNewMetricToDb() {
    JobInfo job =
        new JobInfo(
            "jobId",
            "extId1",
            "Streaming",
            "DataflowRunner",
            new Store(),
            Collections.emptyList(),
            Collections.emptyList(),
            JobStatus.RUNNING);

    Metrics metric1 = new Metrics(job, "metric1", 1);
    Metrics metric2 = new Metrics(job, "metric2", 2);
    List<Metrics> metrics = Arrays.asList(metric1, metric2);

    when(jobInfoRepository.findByStatusNotIn((Collection<JobStatus>) any(Collection.class)))
        .thenReturn(Arrays.asList(job));
    when(jobMonitor.getJobMetrics(job)).thenReturn(metrics);

    scheduledJobMonitor.getJobMetrics();

    verify(stasdMetricPusher).pushMetrics(metrics);
    ArgumentCaptor<JobInfo> argCaptor = ArgumentCaptor.forClass(JobInfo.class);
    verify(jobInfoRepository).save(argCaptor.capture());

    assertThat(job.getMetrics(), equalTo(metrics));
  }
}
