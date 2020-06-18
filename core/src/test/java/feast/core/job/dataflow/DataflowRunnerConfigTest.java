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
package feast.core.job.dataflow;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.*;

import com.google.common.collect.Lists;
import feast.proto.core.RunnerProto.DataflowRunnerConfigOptions;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class DataflowRunnerConfigTest {
  @Test
  public void shouldConvertToPipelineArgs() throws IllegalAccessException {
    DataflowRunnerConfigOptions opts =
        DataflowRunnerConfigOptions.newBuilder()
            .setProject("my-project")
            .setRegion("asia-east1")
            .setZone("asia-east1-a")
            .setTempLocation("gs://bucket/tempLocation")
            .setNetwork("default")
            .setSubnetwork("regions/asia-east1/subnetworks/mysubnetwork")
            .setMaxNumWorkers(1)
            .setAutoscalingAlgorithm("THROUGHPUT_BASED")
            .setUsePublicIps(false)
            .setWorkerMachineType("n1-standard-1")
            .setDeadLetterTableSpec("project_id:dataset_id.table_id")
            .putLabels("key", "value")
            .build();

    DataflowRunnerConfig dataflowRunnerConfig = new DataflowRunnerConfig(opts);
    List<String> args = Lists.newArrayList(dataflowRunnerConfig.toArgs());
    String[] expectedArgs =
        Arrays.asList(
                "--project=my-project",
                "--region=asia-east1",
                "--zone=asia-east1-a",
                "--tempLocation=gs://bucket/tempLocation",
                "--network=default",
                "--subnetwork=regions/asia-east1/subnetworks/mysubnetwork",
                "--maxNumWorkers=1",
                "--autoscalingAlgorithm=THROUGHPUT_BASED",
                "--usePublicIps=false",
                "--workerMachineType=n1-standard-1",
                "--deadLetterTableSpec=project_id:dataset_id.table_id",
                "--labels={\"key\":\"value\"}")
            .toArray(String[]::new);
    assertThat(args.size(), equalTo(expectedArgs.length));
    assertThat(args, containsInAnyOrder(expectedArgs));
  }
}
