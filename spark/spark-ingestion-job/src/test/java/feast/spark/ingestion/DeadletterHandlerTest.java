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
package feast.spark.ingestion;

import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.collect.ImmutableMap;
import feast.common.models.FeatureSetReference;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.spark.ingestion.common.FailedElement;
import feast.test.TestUtil.DummyStatsDServer;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class DeadletterHandlerTest {

  public static final String STATSD_SERVER_HOST = "localhost";
  public static final int STATSD_SERVER_PORT = 17255;

  public SparkSession sparkSession;
  public DummyStatsDServer statsDServer;

  @TempDir public Path deadLetterFolder;

  @BeforeEach
  public void setupSpark() {
    sparkSession =
        SparkSession.builder()
            .appName(getClass().getName())
            .master("local")
            .config(
                "spark.metrics.conf.*.sink.statsd.class",
                "org.apache.spark.metrics.sink.StatsdSink")
            .config("spark.metrics.namespace", "feast_ingestion")
            .config("spark.metrics.staticSources.enabled", "false")
            .config("spark.metrics.conf.*.sink.statsd.period", "1")
            .config("spark.metrics.conf.*.sink.statsd.host", STATSD_SERVER_HOST)
            .config("spark.metrics.conf.*.sink.statsd.port", STATSD_SERVER_PORT)
            .getOrCreate();
  }

  @BeforeEach
  public void setupStatsD() {
    statsDServer = new DummyStatsDServer(STATSD_SERVER_PORT);
  }

  @AfterEach
  public void teardownSpark() {
    if (sparkSession != null) {
      this.sparkSession.close();
    }
  }

  @AfterEach
  public void teardownStatsD() {
    if (statsDServer != null) {
      statsDServer.stop();
    }
  }

  @Test
  public void shouldSendCorrectStatsDMetrics() throws InterruptedException {
    FeatureSetReference featureSetRef = FeatureSetReference.parse("project/test");
    FeatureSetSpec featureSetSpec =
        FeatureSetSpec.newBuilder().setProject("project").setName("test").build();
    String deadLetterPath = deadLetterFolder.toAbsolutePath().toString();
    DeadLetterHandler deadLetterHandler =
        new DeadLetterHandler(
            deadLetterPath, "job", ImmutableMap.of(featureSetRef, featureSetSpec));

    Dataset<FailedElement> input =
        sparkSession.createDataset(
            Collections.singletonList(
                FailedElement.newBuilder()
                    .setProjectName("project")
                    .setFeatureSetName("test")
                    .setJobName("job")
                    .setTransformName("WriteFailureMetrics")
                    .setErrorMessage("Error")
                    .build()),
            Encoders.kryo(FailedElement.class));

    deadLetterHandler.storeDeadLetter(input);

    // Wait until StatsD has finished processed all messages
    Thread.sleep(3000);

    List<String> actualLines = statsDServer.messagesReceived();

    List<String> expectedLines =
        Collections.singletonList(
            "feast_ingestion.driver.FeastMetrics.deadletter_row_count#ingestion_job_name=job,feast_project_name=project,feast_featureSet_name=test:1|g");

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
  }
}
