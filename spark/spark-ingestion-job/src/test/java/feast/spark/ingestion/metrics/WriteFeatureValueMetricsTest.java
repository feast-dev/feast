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
import feast.proto.core.FeatureSetProto;
import feast.proto.types.FeatureRowProto;
import feast.proto.types.FieldProto;
import feast.spark.ingestion.metrics.sources.FeastMetricsSource;
import feast.spark.ingestion.metrics.utils.MetricsSourceUtil;
import feast.spark.ingestion.metrics.writers.FeastMetricsWriter;
import java.io.IOException;
import java.util.*;
import org.apache.spark.SparkEnv;
import org.apache.spark.metrics.MetricsSystem;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class WriteFeatureValueMetricsTest {

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
  public void shouldSendCorrectStatsDMetrics() throws InterruptedException, IOException {

    Map<FeatureSetReference, Iterable<FeatureRowProto.FeatureRow>> input =
        readTestInput("feast/spark/ingestion/metrics/WriteFeatureValueMetricsTest.input");
    List<String> expectedLines =
        readTestOutput("feast/spark/ingestion/metrics/WriteInflightFeatureValueMetricsTest.output");

    FeatureSetReference featuresetRef = FeatureSetReference.of("project", "featureset");
    Iterable<FeatureRowProto.FeatureRow> featureRows = input.get(featuresetRef);
    List<FieldProto.Field> featureNames = featureRows.iterator().next().getFieldsList();

    String metricsNamespace = FeastMetricsWriter.METRICS_NAMESPACE;
    String jobId = "job";
    String storeName = "store";

    MetricsSystem system = SparkEnv.get().metricsSystem();

    FeastMetricsSource metricsSource = new FeastMetricsSource();
    MetricsSourceUtil.registerFeatureRowCountMetrics(
        metricsNamespace,
        featuresetRef,
        jobId,
        storeName,
        metricsSource.metricRegistry(),
        metricsSource.getRowCountGaugeMap());

    for (FieldProto.Field featureName : featureNames) {
      MetricsSourceUtil.registerFeatureValueMetrics(
          metricsNamespace,
          featuresetRef,
          featureName.getName(),
          jobId,
          storeName,
          metricsSource.metricRegistry(),
          metricsSource.getFeatureValueMetricsMap());
    }
    system.registerSource(metricsSource);

    WriteFeatureValueMetrics writeFeatureValueMetrics = new WriteFeatureValueMetrics(metricsSource);

    Map<FeatureSetReference, FeatureSetProto.FeatureSetSpec> featureSetSpecMap = new HashMap<>();
    featureSetSpecMap.put(featuresetRef, FeatureSetProto.FeatureSetSpec.newBuilder().build());

    writeFeatureValueMetrics.processMetric(
        metricsNamespace, featuresetRef, storeName, featureRows, featureSetSpecMap);

    // Wait until StatsD has finished processed all messages
    Thread.sleep(3000);

    List<String> actualLines = statsDServer.messagesReceived();

    List<String> listWithoutDuplicates = new ArrayList<>(new HashSet<>(actualLines));

    statsDMessagesCompare(expectedLines, listWithoutDuplicates);
  }
}
