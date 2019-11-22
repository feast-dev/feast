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

import feast.core.SourceProto.KafkaSourceConfig;
import feast.core.SourceProto.SourceType;
import feast.core.dao.JobInfoRepository;
import feast.core.model.JobInfo;
import feast.core.model.JobStatus;
import feast.core.model.Source;
import feast.core.model.Store;
import java.util.Collection;
import java.util.Collections;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ScheduledJobMonitorTest {

  ScheduledJobMonitor scheduledJobMonitor;

  @Mock JobMonitor jobMonitor;

  @Mock JobInfoRepository jobInfoRepository;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    scheduledJobMonitor = new ScheduledJobMonitor(jobMonitor, jobInfoRepository);
  }

  @Test
  public void getJobStatus_shouldUpdateJobInfoForRunningJob() {
    Source source =
        new Source(
            SourceType.KAFKA,
            KafkaSourceConfig.newBuilder()
                .setBootstrapServers("kafka:9092")
                .setTopic("feast-topic")
                .build(),
            true);
    JobInfo job =
        new JobInfo(
            "jobId",
            "extId1",
            "DataflowRunner",
            source,
            new Store(),
            Collections.emptyList(),
            Collections.emptyList(),
            JobStatus.RUNNING);

    when(jobInfoRepository.findByStatusNotIn((Collection<JobStatus>) any(Collection.class)))
        .thenReturn(Collections.singletonList(job));
    when(jobMonitor.getJobStatus(job)).thenReturn(JobStatus.COMPLETED);

    scheduledJobMonitor.updateJobStatus();

    ArgumentCaptor<JobInfo> argCaptor = ArgumentCaptor.forClass(JobInfo.class);
    verify(jobInfoRepository).save(argCaptor.capture());

    JobInfo jobInfos = argCaptor.getValue();
    assertThat(jobInfos.getStatus(), equalTo(JobStatus.COMPLETED));
  }

  @Test
  public void getJobStatus_shouldNotUpdateJobInfoForTerminalJob() {
    when(jobInfoRepository.findByStatusNotIn((Collection<JobStatus>) any(Collection.class)))
        .thenReturn(Collections.emptyList());

    scheduledJobMonitor.updateJobStatus();

    verify(jobInfoRepository, never()).save(any(JobInfo.class));
  }
}
