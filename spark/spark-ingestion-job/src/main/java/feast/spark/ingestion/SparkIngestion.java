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
import feast.proto.core.FeatureSetProto.EntitySpec;
import feast.proto.core.FeatureSetProto.FeatureSet;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.SourceProto.KafkaSourceConfig;
import feast.proto.core.StoreProto.Store;
import feast.proto.core.StoreProto.Store.DeltaConfig;
import feast.proto.core.StoreProto.Store.RedisConfig;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.spark.ingestion.delta.FeatureRowToSparkRow;
import feast.storage.connectors.redis.writer.RedisCustomIO;
import feast.storage.connectors.redis.writer.RedisCustomIO.Write;
import io.delta.tables.DeltaTable;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.codec.Charsets;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Consumes messages from one or more topics in Kafka and outputs them to the console. */
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
  private static final Logger LOGGER = LoggerFactory.getLogger(SparkIngestion.class.getName());
  private static final int DEFAULT_TIMEOUT = 2000;
  private String jobId;
  private String defaultFeastProject;
  private List<FeatureSet> featureSets;
  private List<Store> stores;

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
    LOGGER.info("Feature sets: {}", featureSets);
    LOGGER.info("Stores: {}", stores);
  }

  public StreamingQuery createQuery() {

    // Create session with getOrCreate and do not call SparkContext.stop() at the end.
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

    Collection<VoidFunction2<Dataset<byte[]>, Long>> consumers =
        stores.stream()
            .map(
                store -> {
                  List<FeatureSet> subscribedFeatureSets =
                      SpecUtil.getSubscribedFeatureSets(store.getSubscriptionsList(), featureSets);

                  Map<String, FeatureSetSpec> featureSetSpecsByKey = new HashMap<>();
                  subscribedFeatureSets.stream()
                      .forEach(
                          fs -> {
                            String ref = getFeatureSetReference(fs.getSpec());
                            featureSetSpecsByKey.put(ref, fs.getSpec());
                          });

                  switch (store.getType()) {
                    case DELTA:
                      return deltaSink(store.getDeltaConfig(), spark, featureSetSpecsByKey);
                    case REDIS:
                      return redisSink(store.getRedisConfig(), spark, featureSetSpecsByKey);
                    default:
                      throw new UnsupportedOperationException(
                          "Store " + store + " is not implemented in Spark ingestor");
                  }
                })
            .collect(Collectors.toList());

    ProcessFeatureRowDoFn procFeat = new ProcessFeatureRowDoFn(defaultFeastProject);

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
              consumers.forEach(
                  c -> {
                    try {
                      c.call(batchDF, batchId);
                    } catch (Exception e) {
                      LOGGER.error("Error invoking sink", e);
                      throw new RuntimeException(e);
                    }
                  });
              batchDF.unpersist();
            })
        .start();
  }

  @SuppressWarnings("serial")
  private static class FeatureSetInfo implements Serializable {
    private final String key;
    private final FeatureSetSpec spec;
    private final StructType schema;
    private final String table;

    private FeatureSetInfo(String key, FeatureSetSpec spec, StructType schema, String table) {
      this.key = key;
      this.spec = spec;
      this.schema = schema;
      this.table = table;
    }
  }

  private VoidFunction2<Dataset<byte[]>, Long> deltaSink(
      DeltaConfig deltaConfig,
      SparkSession spark,
      Map<String, FeatureSetSpec> featureSetSpecsByKey) {
    String deltaPath = deltaConfig.getPath();

    FeatureRowToSparkRow mapper = new FeatureRowToSparkRow(jobId);

    List<FeatureSetInfo> featureSetInfos = new ArrayList<FeatureSetInfo>();

    for (Entry<String, FeatureSetSpec> spec : featureSetSpecsByKey.entrySet()) {

      StructType schema = mapper.buildSchema(spec.getValue());
      LOGGER.info("Table: {} schema: {}", spec.getKey(), schema);

      // Initialize Delta table if needed
      String deltaTablePath = getDeltaTablePath(deltaPath, spec.getValue());
      spark
          .createDataFrame(Collections.emptyList(), schema)
          .write()
          .format("delta")
          .partitionBy(FeatureRowToSparkRow.EVENT_TIMESTAMP_DAY_COLUMN)
          .mode("append")
          .save(deltaTablePath);

      featureSetInfos.add(
          new FeatureSetInfo(spec.getKey(), spec.getValue(), schema, deltaTablePath));
    }

    return new DeltaSinkFunction(featureSetInfos, mapper);
  }

  @SuppressWarnings("serial")
  private static class DeltaSinkFunction implements VoidFunction2<Dataset<byte[]>, Long> {

    private final List<FeatureSetInfo> featureSetInfos;
    private final FeatureRowToSparkRow mapper;
    private transient Map<String, DeltaTable> deltaTables = new HashMap<>();

    private DeltaSinkFunction(List<FeatureSetInfo> featureSetInfos, FeatureRowToSparkRow mapper) {
      this.featureSetInfos = featureSetInfos;
      this.mapper = mapper;
    }

    @Override
    public void call(Dataset<byte[]> batchDF, Long batchId) throws Exception {
      LOGGER.info("{} entries", featureSetInfos.size());
      for (FeatureSetInfo fsInfo : featureSetInfos) {
        StructType schema = fsInfo.schema;
        if (!deltaTables.containsKey(fsInfo.table)) {
          deltaTables.put(fsInfo.table, DeltaTable.forPath(fsInfo.table));
        }
        DeltaTable deltaTable = deltaTables.get(fsInfo.table);

        Dataset<Row> rows =
            batchDF.flatMap(
                r -> {
                  FeatureRow featureRow = FeatureRow.parseFrom(r);
                  LOGGER.debug(
                      "Comparing key '{}' and '{}'", fsInfo.key, featureRow.getFeatureSet());
                  if (!fsInfo.key.equals(featureRow.getFeatureSet())) {
                    return Collections.emptyIterator();
                  }
                  return Arrays.asList(mapper.apply(fsInfo.spec, featureRow)).iterator();
                },
                RowEncoder.apply(schema));
        if (rows.isEmpty()) {
          LOGGER.info("No rows for {}", fsInfo.key);
          return;
        }

        List<String> entityNames =
            fsInfo.spec.getEntitiesList().stream()
                .map(EntitySpec::getName)
                .collect(Collectors.toList());
        upsertToDelta(deltaTable, rows, batchId, entityNames);
      }
    }

    private void upsertToDelta(
        DeltaTable deltaTable,
        Dataset<Row> microBatchOutputDF,
        long batchId,
        List<String> entityNames) {

      // Create condition predicate, e.g. "s.key1 = t.key1 and s.key2 = t.key2"
      String condition =
          entityNames.stream()
              .map(n -> String.format("s.%s = t.%s", n, n))
              .collect(Collectors.joining(" and "));

      deltaTable
          .as("t")
          .merge(microBatchOutputDF.as("s"), condition)
          .whenMatched()
          .updateAll()
          .whenNotMatched()
          .insertAll()
          .execute();
    }
  }

  public String getDeltaTablePath(String deltaPath, FeatureSetSpec featureSetSpec) {
    return deltaPath
        + "/"
        + sanitize(featureSetSpec.getProject())
        + "/"
        + sanitize(SpecUtil.getFeatureSetReference(featureSetSpec));
  }

  private String sanitize(String unsafeString) {
    return Base64.getEncoder().encodeToString(unsafeString.getBytes(Charsets.UTF_8));
  }

  private VoidFunction2<Dataset<byte[]>, Long> redisSink(
      RedisConfig redisConfig,
      SparkSession spark,
      Map<String, FeatureSetSpec> featureSetSpecsByKey) {

    Write write = new RedisCustomIO.Write(featureSetSpecsByKey);
    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
    Broadcast<Write> fss = sc.broadcast(write);

    return new RedisWriter(
        fss,
        new RedisURI(
            redisConfig.getHost(),
            redisConfig.getPort(),
            java.time.Duration.ofMillis(DEFAULT_TIMEOUT)));
  }

  @SuppressWarnings("serial")
  private static class RedisWriter implements VoidFunction2<Dataset<byte[]>, Long> {

    private final RedisURI uri;
    private final Broadcast<Write> fss;
    private transient RedisAsyncCommands<byte[], byte[]> commands = null;

    private RedisWriter(Broadcast<Write> fss, RedisURI uri) {
      this.uri = uri;
      this.fss = fss;
    }

    @Override
    public void call(Dataset<byte[]> v1, Long v2) throws Exception {

      Write ss = fss.getValue();
      List<RedisFuture<?>> futures = new ArrayList<>();
      v1.foreach(
          r -> {
            if (commands == null) {
              RedisClient redisclient = RedisClient.create(uri);
              StatefulRedisConnection<byte[], byte[]> connection =
                  redisclient.connect(new ByteArrayCodec());
              commands = connection.async();
            }
            FeatureRow featureRow = FeatureRow.parseFrom(r);
            byte[] key = ss.getKey(featureRow);
            if (key != null) {
              byte[] value = ss.getValue(featureRow);
              futures.add(commands.set(key, value));
            }
          });
      try {
        LettuceFutures.awaitAll(60, TimeUnit.SECONDS, futures.toArray(new RedisFuture<?>[0]));
      } finally {
        futures.clear();
      }
    }
  }
}
