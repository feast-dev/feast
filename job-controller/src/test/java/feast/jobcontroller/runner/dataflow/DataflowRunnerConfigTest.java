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
package feast.jobcontroller.runner.dataflow;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import com.google.common.collect.Lists;
import feast.ingestion.options.ImportOptions;
import feast.proto.core.RunnerProto.DataflowRunnerConfigOptions;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.jupiter.api.Test;

public class DataflowRunnerConfigTest {
  @Test
  public void shouldConvertToPipelineArgs() throws IllegalAccessException {
    DataflowRunnerConfigOptions opts =
        DataflowRunnerConfigOptions.newBuilder()
            .setProject("my-project")
            .setRegion("asia-east1")
            .setWorkerZone("asia-east1-a")
            .setEnableStreamingEngine(true)
            .setWorkerDiskType("pd-ssd")
            .setTempLocation("gs://bucket/tempLocation")
            .setNetwork("default")
            .setSubnetwork("regions/asia-east1/subnetworks/mysubnetwork")
            .setMaxNumWorkers(1)
            .setAutoscalingAlgorithm("THROUGHPUT_BASED")
            .setUsePublicIps(false)
            .setWorkerMachineType("n1-standard-1")
            .setDeadLetterTableSpec("project_id:dataset_id.table_id")
            .setDiskSizeGb(100)
            .putLabels("key", "value")
            .putKafkaConsumerProperties("max.poll.records", "1000")
            .putKafkaConsumerProperties("receive.buffer.bytes", "1000000")
            .build();

    DataflowRunnerConfig dataflowRunnerConfig = new DataflowRunnerConfig(opts);
    List<String> args = Lists.newArrayList(dataflowRunnerConfig.toArgs());
    String[] expectedArgs =
        Arrays.asList(
                "--project=my-project",
                "--region=asia-east1",
                "--workerZone=asia-east1-a",
                "--tempLocation=gs://bucket/tempLocation",
                "--network=default",
                "--subnetwork=regions/asia-east1/subnetworks/mysubnetwork",
                "--maxNumWorkers=1",
                "--autoscalingAlgorithm=THROUGHPUT_BASED",
                "--usePublicIps=false",
                "--workerMachineType=n1-standard-1",
                "--deadLetterTableSpec=project_id:dataset_id.table_id",
                "--diskSizeGb=100",
                "--labels={\"key\":\"value\"}",
                "--kafkaConsumerProperties={\"max.poll.records\":\"1000\",\"receive.buffer.bytes\":\"1000000\"}",
                "--enableStreamingEngine=true",
                "--workerDiskType=pd-ssd")
            .toArray(String[]::new);

    assertThat(args.size(), equalTo(expectedArgs.length));
    assertThat(args, containsInAnyOrder(expectedArgs));

    ImportOptions pipelineOptions =
        PipelineOptionsFactory.fromArgs(dataflowRunnerConfig.toArgs()).as(ImportOptions.class);

    assertThat(
        pipelineOptions.getKafkaConsumerProperties(),
        equalTo(opts.getKafkaConsumerPropertiesMap()));
  }

  @Test
  public void shouldIgnoreOptionalArguments() throws IllegalAccessException {
    DataflowRunnerConfigOptions opts =
        DataflowRunnerConfigOptions.newBuilder()
            .setProject("my-project")
            .setRegion("asia-east1")
            .setWorkerZone("asia-east1-a")
            .setTempLocation("gs://bucket/tempLocation")
            .setNetwork("default")
            .setSubnetwork("regions/asia-east1/subnetworks/mysubnetwork")
            .setMaxNumWorkers(1)
            .setAutoscalingAlgorithm("THROUGHPUT_BASED")
            .setUsePublicIps(false)
            .setWorkerMachineType("n1-standard-1")
            .build();

    DataflowRunnerConfig dataflowRunnerConfig = new DataflowRunnerConfig(opts);
    List<String> args = Lists.newArrayList(dataflowRunnerConfig.toArgs());
    String[] expectedArgs =
        Arrays.asList(
                "--project=my-project",
                "--region=asia-east1",
                "--workerZone=asia-east1-a",
                "--tempLocation=gs://bucket/tempLocation",
                "--network=default",
                "--subnetwork=regions/asia-east1/subnetworks/mysubnetwork",
                "--maxNumWorkers=1",
                "--autoscalingAlgorithm=THROUGHPUT_BASED",
                "--usePublicIps=false",
                "--workerMachineType=n1-standard-1",
                "--labels={}",
                "--kafkaConsumerProperties={}",
                "--enableStreamingEngine=false")
            .toArray(String[]::new);

    assertThat(args.size(), equalTo(expectedArgs.length));
    assertThat(args, containsInAnyOrder(expectedArgs));
  }
}
