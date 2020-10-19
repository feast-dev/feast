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
package feast.spark.ingestion.metrics;

import static feast.test.TestUtil.*;

import feast.common.models.FeatureSetReference;
import feast.proto.types.FeatureRowProto;
import feast.proto.types.FieldProto;
import feast.spark.ingestion.metrics.sources.FeastMetricsSource;
import feast.spark.ingestion.metrics.utils.MetricsSourceUtil;
import feast.spark.ingestion.metrics.writers.FeastMetricsWriter;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.spark.SparkEnv;
import org.apache.spark.metrics.MetricsSystem;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class WriteRowMetricsTest {

  public static final String STATSD_SERVER_HOST = "localhost";
  public static final int STATSD_SERVER_PORT = 17255;

  public SparkSession sparkSession;
  public DummyStatsDServer statsDServer;

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
  public void shouldSendCorrectStatsDMetrics() throws IOException, InterruptedException {
    Map<FeatureSetReference, Iterable<FeatureRowProto.FeatureRow>> input =
        readTestInput("feast/spark/ingestion/metrics/WriteRowMetricsTest.input");
    List<String> expectedLines =
        readTestOutput("feast/spark/ingestion/metrics/WriteInflightRowMetricsTest.output");

    FeatureSetReference featuresetRef = FeatureSetReference.parse("project/featureset");
    String metricsNamespace = FeastMetricsWriter.METRICS_NAMESPACE;
    String jobId = "job";
    String storeName = "store";

    MetricsSystem system = SparkEnv.get().metricsSystem();

    Iterable<FeatureRowProto.FeatureRow> featureRows = input.get(featuresetRef);
    List<FieldProto.Field> featureNames = featureRows.iterator().next().getFieldsList();

    FeastMetricsSource metricsSource = new FeastMetricsSource();

    MetricsSourceUtil.registerFeatureRowCountMetrics(
        metricsNamespace,
        featuresetRef,
        jobId,
        storeName,
        metricsSource.metricRegistry(),
        metricsSource.getRowCountGaugeMap());

    MetricsSourceUtil.registerFeatureRowMetrics(
        metricsNamespace,
        featuresetRef,
        jobId,
        storeName,
        metricsSource.metricRegistry(),
        metricsSource.getFeatureRowMetricsMap());

    for (FieldProto.Field featureName : featureNames) {
      MetricsSourceUtil.registerFeatureNameMissingCount(
          metricsNamespace,
          featuresetRef,
          featureName.getName(),
          jobId,
          storeName,
          metricsSource.metricRegistry(),
          metricsSource.getFeatureNameMissingCountMap());
    }

    system.registerSource(metricsSource);

    WriteRowMetrics writeRowMetrics = new WriteRowMetrics(metricsSource);
    writeRowMetrics.processMetric(
        Clock.fixed(Instant.ofEpochSecond(1585548645), ZoneId.of("UTC")),
        metricsNamespace,
        featuresetRef,
        storeName,
        input.get(featuresetRef));

    // Wait until StatsD has finished processed all messages
    Thread.sleep(3000);

    List<String> actualLines = statsDServer.messagesReceived();

    List<String> listWithoutDuplicates = new ArrayList<>(new HashSet<>(actualLines));

    statsDMessagesCompare(expectedLines, listWithoutDuplicates);
  }
}
