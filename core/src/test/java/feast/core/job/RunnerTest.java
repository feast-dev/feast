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
package feast.core.job;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.NoSuchElementException;
import org.junit.Test;

public class RunnerTest {

  @Test
  public void toStringReturnsHumanReadableName() {
    assertThat(Runner.DATAFLOW.toString(), is("DataflowRunner"));
  }

  @Test
  public void fromStringLoadsValueFromHumanReadableName() {
    var humanName = Runner.DATAFLOW.toString();
    assertThat(Runner.fromString(humanName), is(Runner.DATAFLOW));
  }

  @Test(expected = NoSuchElementException.class)
  public void fromStringThrowsNoSuchElementExceptionForUnknownValue() {
    Runner.fromString("this is not a valid Runner");
  }
}
