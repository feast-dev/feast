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
package feast.core.job.direct;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;

import com.google.common.collect.Lists;
import feast.proto.core.RunnerProto.DirectRunnerConfigOptions;
import java.util.List;
import org.junit.Test;

public class DirectRunnerConfigTest {
  @Test
  public void shouldConvertToPipelineArgs() throws IllegalAccessException {
    DirectRunnerConfigOptions opts =
        DirectRunnerConfigOptions.newBuilder()
            .setTargetParallelism(1)
            .setDeadLetterTableSpec("project_id:dataset_id.table_id")
            .build();
    DirectRunnerConfig directRunnerConfig = new DirectRunnerConfig(opts);
    List<String> args = Lists.newArrayList(directRunnerConfig.toArgs());
    assertThat(args.size(), equalTo(2));
    assertThat(
        args,
        containsInAnyOrder(
            "--targetParallelism=1", "--deadletterTableSpec=project_id:dataset_id.table_id"));
  }
}
