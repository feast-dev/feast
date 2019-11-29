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
package feast.core.job.dataflow;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.Dataflow.Projects;
import com.google.api.services.dataflow.Dataflow.Projects.Locations;
import com.google.api.services.dataflow.Dataflow.Projects.Locations.Jobs;
import com.google.api.services.dataflow.Dataflow.Projects.Locations.Jobs.Get;
import com.google.api.services.dataflow.model.Job;
import com.google.common.collect.Lists;
import feast.core.job.Runner;
import feast.core.model.JobInfo;
import feast.core.model.JobStatus;
import feast.types.FieldProto.Field;
import feast.types.ValueProto.BoolList;
import feast.types.ValueProto.Value;
import java.io.IOException;
import org.junit.Before;
import org.junit.Test;

public class DataflowJobMonitorTest {

  private DataflowJobMonitor monitor;
  private String location;
  private String projectId;
  private Jobs jobService;

  @Before
  public void setUp() throws Exception {
    projectId = "myProject";
    location = "asia-east1";
    Dataflow dataflow = mock(Dataflow.class);
    Dataflow.Projects projects = mock(Projects.class);
    Dataflow.Projects.Locations locations = mock(Locations.class);
    jobService = mock(Jobs.class);
    when(dataflow.projects()).thenReturn(projects);
    when(projects.locations()).thenReturn(locations);
    when(locations.jobs()).thenReturn(jobService);

    monitor = new DataflowJobMonitor(dataflow, projectId, location);
  }

  @Test
  public void getJobStatus_shouldReturnCorrectJobStatusForValidDataflowJobState()
      throws IOException {
    String jobId = "myJobId";

    Get getOp = mock(Get.class);
    Job job = mock(Job.class);
    when(getOp.execute()).thenReturn(job);
    when(job.getCurrentState()).thenReturn(DataflowJobState.JOB_STATE_RUNNING.toString());
    when(jobService.get(projectId, location, jobId)).thenReturn(getOp);

    JobInfo jobInfo = mock(JobInfo.class);
    when(jobInfo.getExtId()).thenReturn(jobId);
    when(jobInfo.getRunner()).thenReturn(Runner.DATAFLOW.getName());
    assertThat(monitor.getJobStatus(jobInfo), equalTo(JobStatus.RUNNING));
  }

  @Test
  public void getJobStatus_shouldReturnUnknownStateForInvalidDataflowJobState() throws IOException {
    String jobId = "myJobId";

    Get getOp = mock(Get.class);
    Job job = mock(Job.class);
    when(getOp.execute()).thenReturn(job);
    when(job.getCurrentState()).thenReturn("Random String");
    when(jobService.get(projectId, location, jobId)).thenReturn(getOp);

    JobInfo jobInfo = mock(JobInfo.class);
    when(jobInfo.getExtId()).thenReturn(jobId);
    when(jobInfo.getRunner()).thenReturn(Runner.DATAFLOW.getName());
    assertThat(monitor.getJobStatus(jobInfo), equalTo(JobStatus.UNKNOWN));
  }

  @Test
  public void getJobStatus_shouldReturnUnknownStateWhenExceptionHappen() throws IOException {
    String jobId = "myJobId";

    when(jobService.get(projectId, location, jobId))
        .thenThrow(new RuntimeException("some thing wrong"));

    JobInfo jobInfo = mock(JobInfo.class);
    when(jobInfo.getExtId()).thenReturn(jobId);
    when(jobInfo.getRunner()).thenReturn(Runner.DATAFLOW.getName());
    assertThat(monitor.getJobStatus(jobInfo), equalTo(JobStatus.UNKNOWN));
  }

  @Test
  public void test() {
    Field field =
        Field.newBuilder()
            .setName("Hello")
            .setValue(
                Value.newBuilder()
                    .setBoolListVal(
                        BoolList.newBuilder()
                            .addAllVal(Lists.newArrayList(true, false, true, true))
                            .build()))
            .build();
    field.getName();
  }
}
