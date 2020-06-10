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

import static feast.ingestion.utils.SpecUtil.getFeatureSetReference;

import feast.ingestion.transform.fn.ProcessFeatureRowDoFn;
import feast.ingestion.utils.SpecUtil;
import feast.proto.core.FeatureSetProto.FeatureSet;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.SourceProto.KafkaSourceConfig;
import feast.proto.core.StoreProto.Store;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.spark.ingestion.delta.SparkDeltaSink;
import feast.spark.ingestion.redis.SparkRedisSink;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consumes messages from one or more topics in Kafka and outputs them to Delta/Redis using Spark.
 */
/*
 * Usage:
 * SparkIngestion parameters
 *
 * List of parameters:
 *
 * <job-name> Job name.
 *
 * <default-project> Default feast project to apply to incoming rows that do not specify project in its feature set reference.
 *
 * <feature-sets-json> Feature set definitions (in JSON Lines text format).
 *
 * <stores-json> Store set definitions (in JSON Lines text format).
 *
 */
public class SparkIngestion {
  private static final Logger log = LoggerFactory.getLogger(SparkIngestion.class);
  private final String jobId;
  private final String defaultFeastProject;
  private final List<FeatureSet> featureSets;
  private final List<Store> stores;

  public static void main(String[] args) throws Exception {
    SparkIngestion ingestion = new SparkIngestion(args);
    ingestion.createQuery().awaitTermination();
  }

  public SparkIngestion(String[] args) throws IOException {
    int numArgs = 4;
    if (args.length != numArgs) {
      throw new IllegalArgumentException("Expecting " + numArgs + " arguments");
    }

    int index = 0;
    jobId = args[index++];
    defaultFeastProject = args[index++];
    String featureSetSpecsJson = args[index++];
    String storesJson = args[index++];

    featureSets =
        SpecUtil.parseFeatureSetSpecJsonList(Arrays.asList(featureSetSpecsJson.split("\n")));
    stores = SpecUtil.parseStoreJsonList(Arrays.asList(storesJson.split("\n")));
    log.info("Feature sets: {}", featureSets);
    log.info("Stores: {}", stores);
  }

  public StreamingQuery createQuery() {

    // Create session with getOrCreate and do not call SparkContext.stop() nor System.exit() at the
    // end.
    // See https://docs.databricks.com/jobs.html#jar-job-tips
    SparkSession spark = SparkSession.builder().appName("SparkIngestion").getOrCreate();

    Set<KafkaSourceConfig> kafkaConfigs =
        featureSets.stream()
            .map(s -> s.getSpec().getSource().getKafkaSourceConfig())
            .collect(Collectors.toSet());
    if (kafkaConfigs.size() != 1) {
      throw new UnsupportedOperationException(
          "Single Kafka config required, got " + kafkaConfigs.size());
    }
    KafkaSourceConfig kafkaConfig = kafkaConfigs.iterator().next();

    // Create DataSet representing the stream of input lines from kafka
    Dataset<Row> input =
        spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaConfig.getBootstrapServers())
            .option("subscribe", kafkaConfig.getTopic())
            .load();

    Collection<SparkSink> consumers =
        stores.stream()
            .map(
                store -> {
                  List<FeatureSet> subscribedFeatureSets =
                      SpecUtil.getSubscribedFeatureSets(store.getSubscriptionsList(), featureSets);

                  Map<String, FeatureSetSpec> featureSetSpecsByKey =
                      subscribedFeatureSets.stream()
                          .collect(
                              Collectors.toMap(
                                  fs -> getFeatureSetReference(fs.getSpec()), fs -> fs.getSpec()));

                  switch (store.getType()) {
                    case DELTA:
                      return new SparkDeltaSink(
                          jobId, store.getDeltaConfig(), spark, featureSetSpecsByKey);
                    case REDIS:
                      return new SparkRedisSink(
                          store.getRedisConfig(), spark, featureSetSpecsByKey);
                    default:
                      throw new UnsupportedOperationException(
                          "Store " + store + " is not implemented in Spark ingestor");
                  }
                })
            .collect(Collectors.toList());

    ProcessFeatureRowDoFn procFeat = new ProcessFeatureRowDoFn(defaultFeastProject);

    List<VoidFunction2<Dataset<byte[]>, Long>> consumerSinks =
        consumers.stream()
            .map(
                c -> {
                  try {
                    return c.configure();
                  } catch (Exception e) {
                    log.error("Error configuring sink", e);
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toList());

    // Start running the query that writes the data to Delta Lake
    return input
        .select("value")
        .map(
            r -> {
              FeatureRow featureRow = FeatureRow.parseFrom((byte[]) r.getAs(0));
              featureRow = procFeat.processElement(featureRow);
              return featureRow.toByteArray();
            },
            Encoders.BINARY())
        .writeStream()
        .foreachBatch(
            (batchDF, batchId) -> {
              batchDF.persist();
              consumerSinks.forEach(
                  c -> {
                    try {
                      c.call(batchDF, batchId);
                    } catch (Exception e) {
                      log.error("Error invoking sink", e);
                      throw new RuntimeException(e);
                    }
                  });
              batchDF.unpersist();
            })
        .start();
  }
}
