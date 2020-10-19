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

import static feast.test.TestUtil.statsDMessagesCompare;

import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import feast.common.models.FeatureSetReference;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.FeatureSetProto.EntitySpec;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.FeatureSetProto.FeatureSpec;
import feast.proto.types.FeatureRowProto;
import feast.proto.types.FieldProto;
import feast.proto.types.ValueProto;
import feast.proto.types.ValueProto.ValueType.Enum;
import feast.spark.ingestion.metrics.writers.FeastMetricsWriter;
import feast.test.TestUtil.DummyStatsDServer;
import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FeastMetricsWriterTest {

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
  public void sendCorrectStatsDMetrics() throws InterruptedException {

    FeastMetricsWriter inflightMetrics =
        new FeastMetricsWriter("job", Collections.singletonList("store"), Clock.systemUTC());

    Dataset<FeatureRowProto.FeatureRow> input =
        sparkSession.createDataset(
            Collections.singletonList(
                FeatureRowProto.FeatureRow.newBuilder()
                    .setFeatureSet("project/feature_set:1")
                    .addFields(
                        FieldProto.Field.newBuilder()
                            .setName("a_feature")
                            .setValue(ValueProto.Value.newBuilder().setInt32Val(2)))
                    .setEventTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                    .build()),
            Encoders.kryo(FeatureRowProto.FeatureRow.class));

    Map<FeatureSetReference, FeatureSetProto.FeatureSetSpec> featureSetSpecMap = new HashMap<>();
    FeatureSetSpec featureSetSpec =
        FeatureSetSpec.newBuilder()
            .setName("feature_set:1")
            .setProject("project")
            .setMaxAge(Durations.fromDays(1))
            .addEntities(
                EntitySpec.newBuilder().setName("an_entity").setValueType(Enum.INT32).build())
            .addFeatures(
                FeatureSpec.newBuilder().setName("a_feature").setValueType(Enum.INT32).build())
            .build();
    featureSetSpecMap.put(FeatureSetReference.of("project", "feature_set:1"), featureSetSpec);

    inflightMetrics.writeMetrics("store", input, featureSetSpecMap);

    // Wait until StatsD has finished processed all messages
    Thread.sleep(3000);

    List<String> actualLines = statsDServer.messagesReceived();

    List<String> expectedLines =
        Collections.singletonList(
            "feast_ingestion.driver.FeastMetrics.feature_row_ingested_count#metrics_namespace=Inflight,ingestion_job_name=job,feast_project_name=project,feast_featureSet_name=feature_set:1,feast_store=store:1|g");

    statsDMessagesCompare(expectedLines, actualLines);
  }
}
