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

import java.io.IOException;
import java.nio.file.Files;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

/** Consumes messages from one or more topics in Kafka and outputs them to the console. */
/*
 * Usage:
 * SparkIngestion <bootstrap-servers> <topics> <sink-type> <sink-location> [<checkpoint-location>]
 *
 * <bootstrap-servers> The Kafka "bootstrap.servers" configuration. A comma-separated list of host:port.
 *
 * <topics> Comma-separated list of topics.
 *
 * <sink-type> Sink type. Must be "delta".
 *
 * <sink-location> Sink location. Must be a storage path.
 *
 * <checkpoint-location> Directory in which to create checkpoints. If not provided,
 * defaults to a randomized directory in /tmp.
 *
 */
public class SparkIngestion {
  private final String bootstrapServers;
  private final String topics;
  private final String sinkType;
  private final String sinkLocation;
  private final String checkpointLocation;

  public static void main(String[] args) throws Exception {
    SparkIngestion ingestion = new SparkIngestion(args);
    ingestion.createQuery().awaitTermination();
  }

  public SparkIngestion(String[] args) throws IOException {
    int numArgs = 4;
    if (args.length < numArgs) {
      System.err.println(
          "Usage: SparkIngestion <bootstrap-servers> "
              + "<topics> <sink-type> <sink-location> [<checkpoint-location>]");
      System.exit(1);
    }

    bootstrapServers = args[0];
    topics = args[1];
    sinkType = args[2];
    sinkLocation = args[3];
    if (args.length > numArgs) {
      checkpointLocation = args[numArgs];
    } else {
      checkpointLocation = Files.createTempDirectory("checkpoint").toString();
    }
  }

  public StreamingQuery createQuery() {

    // Create session with getOrCreate and do not call SparkContext.stop() at the end.
    // See https://docs.databricks.com/jobs.html#jar-job-tips
    SparkSession spark = SparkSession.builder().appName("SparkIngestion").getOrCreate();

    // Create DataSet representing the stream of input lines from kafka
    Dataset<Row> input =
        spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", bootstrapServers)
            .option("subscribe", topics)
            .load();

    // Start running the query that writes the data to Delta Lake
    return input
        .writeStream()
        .option("checkpointLocation", checkpointLocation)
        .format(sinkType)
        .start(sinkLocation);
  }
}
