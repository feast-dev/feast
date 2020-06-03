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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.*;

import feast.databricks.emulator.DatabricksEmulator.*;
import feast.databricks.types.*;
import java.util.Collections;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkAppHandle.State;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import spark.*;

/** Unit test for simple App. */
public class DatabricksEmulatorTest {

  private static final String SAMPLE_RUN_JSON =
      "{\n"
          + "  \"run_name\": \"my spark task\",\n"
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
          + "    \"main_class_name\": \"org.apache.spark.examples.SparkPi\"\n"
          + "  }\n"
          + "}\n"
          + "";

  @Mock SparkAppFactory appFactory;

  @Mock Request request;

  @Mock Response response;

  @Mock SparkAppHandle handle;

  @Mock RunTracker runTracker;

  private EmulatorService emulator;

  @Before
  public void setUp() throws Exception {
    initMocks(this);
    emulator = new EmulatorService();
    emulator.appFactory = appFactory;
    when(appFactory.createApp(anyList(), anyString())).thenReturn(handle);
  }

  @Test
  public void runsSubmitShouldCreateSparkJob() throws Exception {
    // Arrange
    when(request.body()).thenReturn(SAMPLE_RUN_JSON);

    // Act
    emulator.runsSubmit(request, response);

    // Assert
    verify(appFactory)
        .createApp(
            Collections.singletonList(
                "/spark-2.4.5-bin-hadoop2.7/examples/jars/spark-examples_2.11-2.4.5.jar"),
            "org.apache.spark.examples.SparkPi");
  }

  @Test
  public void runsSubmitShouldReturnIncrementingRunId() throws Exception {
    // Arrange
    when(request.body()).thenReturn(SAMPLE_RUN_JSON);

    // Act
    RunsSubmitResponse result1 = emulator.runsSubmit(request, response);
    RunsSubmitResponse result2 = emulator.runsSubmit(request, response);

    // Assert
    assertThat(result1.getRun_id(), equalTo(1L));
    assertThat(result2.getRun_id(), equalTo(2L));
  }

  @Test
  public void runsGetShouldReturnJobState() throws Exception {
    // Arrange
    when(request.queryParams("run_id")).thenReturn("45");

    emulator.runTracker = runTracker;
    when(runTracker.getRun(45)).thenReturn(handle);
    when(handle.getState()).thenReturn(State.FINISHED);

    // Act
    RunsGetResponse result1 = emulator.runsGet(request, response);

    // Assert
    assertThat(result1.getState().getLife_cycle_state(), equalTo(RunLifeCycleState.TERMINATED));
    assertThat(result1.getState().getResult_state(), equalTo(RunResultState.SUCCESS));
  }
}
