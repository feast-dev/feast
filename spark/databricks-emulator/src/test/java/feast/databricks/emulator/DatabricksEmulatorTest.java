/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
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
package feast.databricks.emulator;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.*;

import feast.databricks.emulator.DatabricksEmulator.*;
import feast.databricks.types.*;
import java.util.Arrays;
import java.util.Collections;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkAppHandle.State;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import spark.*;

/** Unit test for simple App. */
public class DatabricksEmulatorTest {

  private static final String SAMPLE_JOB_JSON =
      "{\n"
          + "  \"name\": \"my spark task\",\n"
          + "  \"new_cluster\": {\n"
          + "    \"spark_version\": \"5.3.x-scala2.11\",\n"
          + "    \"node_type_id\": \"Standard_DS1_v2\",\n"
          + "    \"num_workers\": 10\n"
          + "  },\n"
          + "  \"libraries\": [\n"
          + "    {\n"
          + "      \"jar\": \"/spark-2.4.5-bin-hadoop2.7/examples/jars/spark-examples_2.11-2.4.5.jar\"\n"
          + "    }\n"
          + "  ],\n"
          + "  \"spark_jar_task\": {\n"
          + "    \"main_class_name\": \"org.apache.spark.examples.SparkPi\",\n"
          + "    \"parameters\": [\"100\"]\n"
          + "  }\n"
          + "}\n"
          + "";

  @Mock SparkAppFactory appFactory;

  @Mock Request jobRequest;

  @Mock Request request;

  @Mock Response response;

  @Mock SparkAppHandle handle;

  @Mock ItemTracker<SparkAppHandle> runTracker;

  private EmulatorService emulator;

  private JobsCreateResponse job;

  private String runSubmitJson;

  @Before
  public void setUp() throws Exception {
    initMocks(this);
    emulator = new EmulatorService();
    emulator.appFactory = appFactory;
    when(appFactory.createApp(anyList(), anyString(), anyList())).thenReturn(handle);

    when(jobRequest.body()).thenReturn(SAMPLE_JOB_JSON);
    job = emulator.jobsCreate(jobRequest, response);
    runSubmitJson = String.format("{\"job_id\":%s}", job.getJobId());
  }

  @Test
  public void runNowShouldCreateSparkJob() throws Exception {
    // Arrange
    when(request.body()).thenReturn(runSubmitJson);

    // Act
    emulator.runNow(request, response);

    // Assert
    verify(appFactory)
        .createApp(
            Collections.singletonList(
                "/spark-2.4.5-bin-hadoop2.7/examples/jars/spark-examples_2.11-2.4.5.jar"),
            "org.apache.spark.examples.SparkPi",
            Arrays.asList("100"));
  }

  @Test
  public void runNowShouldOverrideSparkJob() throws Exception {
    // Arrange
    runSubmitJson = String.format("{\"job_id\":%s, \"jar_params\":[\"200\"]}", job.getJobId());
    when(request.body()).thenReturn(runSubmitJson);

    // Act
    emulator.runNow(request, response);

    // Assert
    verify(appFactory).createApp(anyList(), anyString(), eq(Arrays.asList("200")));
  }

  @Test
  public void jobsCreateShouldReturnIncrementingJobId() throws Exception {
    // Act
    JobsCreateResponse job2 = emulator.jobsCreate(jobRequest, response);

    // Assert
    assertThat(job.getJobId(), equalTo(1L));
    assertThat(job2.getJobId(), equalTo(2L));
  }

  @Test
  public void runNowShouldReturnIncrementingRunId() throws Exception {
    // Arrange
    when(request.body()).thenReturn(runSubmitJson);

    // Act
    RunNowResponse result1 = emulator.runNow(request, response);
    RunNowResponse result2 = emulator.runNow(request, response);

    // Assert
    assertThat(result1.getRunId(), equalTo(1L));
    assertThat(result2.getRunId(), equalTo(2L));
  }

  @Test
  public void runsGetShouldReturnJobState() throws Exception {
    // Arrange
    when(request.queryParams("run_id")).thenReturn("45");

    emulator.runTracker = runTracker;
    when(runTracker.getItem(45)).thenReturn(handle);
    when(handle.getState()).thenReturn(State.FINISHED);

    // Act
    RunsGetResponse result1 = emulator.runsGet(request, response);

    // Assert
    assertThat(result1.getState().getLifeCycleState(), equalTo(RunLifeCycleState.TERMINATED));
    assertThat(result1.getState().getResultState(), equalTo(RunResultState.SUCCESS));
  }

  @Test
  public void jobsDeleteShouldMakeJobNotRunnable() throws Exception {
    // Arrange
    when(request.body()).thenReturn(runSubmitJson);

    // Act
    emulator.runNow(request, response); // run for existing job, should succeed
    emulator.jobsDelete(request, response);
    try {
      emulator.runNow(request, response); // run for deleted job, should fail
      fail("Should have failed, as job was deleted");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void runsCancelShouldStopSparkApplication() throws Exception {
    // Arrange
    when(request.body()).thenReturn(String.format("{\"run_id\":%s}", 45));
    emulator.runTracker = runTracker;
    when(runTracker.getItem(45)).thenReturn(handle);

    // Act
    emulator.runsCancel(request, response);

    // Assert
    verify(handle).stop();
  }
}
