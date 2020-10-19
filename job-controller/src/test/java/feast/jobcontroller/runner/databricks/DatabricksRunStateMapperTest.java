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
package feast.jobcontroller.runner.databricks;

import static feast.databricks.types.RunLifeCycleState.INTERNAL_ERROR;
import static feast.databricks.types.RunLifeCycleState.PENDING;
import static feast.databricks.types.RunLifeCycleState.RUNNING;
import static feast.databricks.types.RunLifeCycleState.SKIPPED;
import static feast.databricks.types.RunLifeCycleState.TERMINATED;
import static feast.databricks.types.RunLifeCycleState.TERMINATING;
import static feast.databricks.types.RunResultState.CANCELED;
import static feast.databricks.types.RunResultState.FAILED;
import static feast.databricks.types.RunResultState.SUCCESS;
import static feast.databricks.types.RunResultState.TIMEDOUT;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import feast.databricks.types.RunLifeCycleState;
import feast.databricks.types.RunResultState;
import feast.databricks.types.RunState;
import feast.databricks.types.RunState.Builder;
import feast.jobcontroller.model.JobStatus;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class DatabricksRunStateMapperTest {

  private RunState state;

  private JobStatus expectedStatus;

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {

          // If life_cycle_state = TERMINATING or lifecyclestate = INTERNAL_ERROR
          // the result state is available if the run had a task and managed to start it.

          {test(PENDING, empty()), JobStatus.PENDING},
          {test(RUNNING, empty()), JobStatus.RUNNING},
          {test(TERMINATING, empty()), JobStatus.ABORTING},
          {test(TERMINATING, of(CANCELED)), JobStatus.ABORTED},
          {test(TERMINATING, of(FAILED)), JobStatus.ERROR},
          {test(TERMINATED, of(SUCCESS)), JobStatus.COMPLETED},
          {test(TERMINATED, of(FAILED)), JobStatus.ERROR},
          {test(TERMINATED, of(TIMEDOUT)), JobStatus.ABORTED},
          {test(TERMINATED, of(CANCELED)), JobStatus.ABORTED},
          {test(SKIPPED, empty()), JobStatus.ABORTED},
          {test(INTERNAL_ERROR, empty()), JobStatus.ERROR},
          {test(INTERNAL_ERROR, of(FAILED)), JobStatus.ERROR},
        });
  }

  private static RunState test(RunLifeCycleState state, Optional<RunResultState> result) {
    Builder builder = RunState.builder().setLifeCycleState(state);
    result.ifPresent(r -> builder.setResultState(r));
    return builder.build();
  }

  public DatabricksRunStateMapperTest(RunState input, JobStatus expected) {
    state = input;
    expectedStatus = expected;
  }

  @Test
  public void testMapper() throws Exception {
    JobStatus jobStatus = DatabricksRunStateMapper.mapJobState(state);
    assertThat(jobStatus, equalTo(expectedStatus));
  }
}
