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
package feast.ingestion.transform.metrics;

import static feast.ingestion.transform.metrics.WriteFeatureValueMetricsDoFnTest.readTestInput;
import static feast.ingestion.transform.metrics.WriteFeatureValueMetricsDoFnTest.readTestOutput;
import static org.junit.Assert.fail;

import feast.test.TestUtil.DummyStatsDServer;
import feast.types.FeatureRowProto.FeatureRow;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.Rule;
import org.junit.Test;

public class WriteRowMetricsDoFnTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  private static final int STATSD_SERVER_PORT = 17255;
  private final DummyStatsDServer statsDServer = new DummyStatsDServer(STATSD_SERVER_PORT);

  @Test
  public void shouldSendCorrectStatsDMetrics() throws IOException, InterruptedException {
    PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
    pipelineOptions.setJobName("job");
    Map<String, Iterable<FeatureRow>> input =
        readTestInput("feast/ingestion/transform/WriteRowMetricsDoFnTest.input");
    List<String> expectedLines =
        readTestOutput("feast/ingestion/transform/WriteRowMetricsDoFnTest.output");

    pipeline
        .apply(Create.of(input))
        .apply(
            ParDo.of(
                WriteRowMetricsDoFn.newBuilder()
                    .setStatsdHost("localhost")
                    .setStatsdPort(STATSD_SERVER_PORT)
                    .setStoreName("store")
                    .setClock(Clock.fixed(Instant.ofEpochSecond(1585548645), ZoneId.of("UTC")))
                    .build()));
    pipeline.run(pipelineOptions).waitUntilFinish();
    // Wait until StatsD has finished processed all messages, 3 sec is a reasonable duration
    // based on empirical testing.
    Thread.sleep(3000);

    List<String> actualLines = statsDServer.messagesReceived();
    for (String expected : expectedLines) {
      boolean matched = false;
      for (String actual : actualLines) {
        if (actual.equals(expected)) {
          matched = true;
          break;
        }
      }
      if (!matched) {
        System.out.println("Print actual metrics output for debugging:");
        for (String line : actualLines) {
          System.out.println(line);
        }
        fail(String.format("Expected StatsD metric not found:\n%s", expected));
      }
    }
    statsDServer.stop();
  }
}
