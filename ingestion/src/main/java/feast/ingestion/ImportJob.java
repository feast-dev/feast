/*
 * Copyright 2018 The Feast Authors
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
 *
 */

package feast.ingestion;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import feast.ingestion.boot.ImportJobModule;
import feast.ingestion.boot.PipelineModule;
import feast.ingestion.metrics.FeastMetrics;
import feast.ingestion.options.ImportJobPipelineOptions;
import feast.ingestion.transform.ErrorsStoreTransform;
import feast.ingestion.transform.ReadFeaturesTransform;
import feast.ingestion.transform.ToFeatureRowExtended;
import feast.ingestion.transform.WriteFeaturesTransform;
import feast.ingestion.transform.fn.LoggerDoFn;
import feast.ingestion.util.ProtoUtil;
import feast.ingestion.util.SchemaUtil;
import feast.ingestion.values.PFeatureRows;
import feast.specs.ImportJobSpecsProto.ImportJobSpecs;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import feast.types.FeatureRowProto.FeatureRow;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.commons.codec.digest.DigestUtils;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.slf4j.event.Level;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.IOException;
import java.util.Random;

@Slf4j
public class ImportJob {

  private static Random random = new Random(System.currentTimeMillis());

  private final Pipeline pipeline;
  private final ImportJobSpecs importJobSpecs;
  private final ReadFeaturesTransform readFeaturesTransform;
  private final WriteFeaturesTransform writeFeaturesTransform;
  // private final ServingStoreTransform servingStoreTransform;
  // private final WarehouseStoreTransform warehouseStoreTransform;
  private final ErrorsStoreTransform errorsStoreTransform;
  // private final boolean dryRun;
  private final ImportJobPipelineOptions options;
  // private final Specs specs;

  @Inject
  public ImportJob(
      Pipeline pipeline,
      ImportJobPipelineOptions options,
      ImportJobSpecs importJobSpecs,
      ReadFeaturesTransform readFeaturesTransform,
      WriteFeaturesTransform writeFeaturesTransform,
      ErrorsStoreTransform errorsStoreTransform) {
    this.pipeline = pipeline;
    this.options = options;
    this.importJobSpecs = importJobSpecs;
    this.readFeaturesTransform = readFeaturesTransform;
    this.writeFeaturesTransform = writeFeaturesTransform;
    this.errorsStoreTransform = errorsStoreTransform;
  }

  // @Inject
  // public ImportJob(
  //     Pipeline pipeline,
  //     ImportJobSpecs importJobSpecs,
  //     ReadFeaturesTransform readFeaturesTransform,
  //     ServingStoreTransform servingStoreTransform,
  //     WarehouseStoreTransform warehouseStoreTransform,
  //     ErrorsStoreTransform errorsStoreTransform,
  //     ImportJobPipelineOptions options,
  //     Specs specs) {
  //   this.pipeline = pipeline;
  //   this.importJobSpecs = importJobSpecs;
  //   this.readFeaturesTransform = readFeaturesTransform;
  //   this.servingStoreTransform = servingStoreTransform;
  //   this.warehouseStoreTransform = warehouseStoreTransform;
  //   this.errorsStoreTransform = errorsStoreTransform;
  //   this.dryRun = options.isDryRun();
  //   this.options = options;
  //   this.specs = specs;
  // }

  public static void main(String[] args) throws IOException {
    mainWithResult(args);
  }

  private static void mainWithResult(String[] args) throws IOException {
    ImportJobPipelineOptions pipelineOptions =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(ImportJobPipelineOptions.class);

    if (pipelineOptions.getJobName().isEmpty()) {
      pipelineOptions.setJobName(generateName());
    }

    ImportJobSpecs importJobSpecs =
        (ImportJobSpecs)
            ProtoUtil.createProtoMessageFromYaml(
                pipelineOptions.getImportJobSpecUri(), ImportJobSpecs.newBuilder());

    setupSink(importJobSpecs);

    Injector injector =
        Guice.createInjector(
            new ImportJobModule(pipelineOptions, importJobSpecs), new PipelineModule());
    ImportJob job = injector.getInstance(ImportJob.class);

    job.expand();
    job.run();
  }

  private static void setupSink(ImportJobSpecs importJobSpecs) {
    StorageSpec sinkStorageSpec = importJobSpecs.getSinkStorageSpec();
    String storageSpecType = sinkStorageSpec.getType();

    switch (storageSpecType) {
      case "BIGQUERY":
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        SchemaUtil.setupBigQuery(
            importJobSpecs.getSinkStorageSpec(),
            importJobSpecs.getEntitySpec(),
            importJobSpecs.getFeatureSpecsList(),
            bigquery);
        break;
      case "REDIS":
        if (!sinkStorageSpec.getOptions().containsKey("host")) {
          throw new IllegalArgumentException(
              "sinkStorageSpec type 'REDIS' requires 'host' options");
        }
        String redisHost = sinkStorageSpec.getOptionsOrThrow("host");
        int redisPort = Integer.parseInt(sinkStorageSpec.getOptionsOrDefault("port", "6379"));
        boolean clusterEnabled =
            Boolean.parseBoolean(sinkStorageSpec.getOptionsOrDefault("clusterEnabled", "false"));
        if (clusterEnabled) {
          throw new IllegalArgumentException(
              "Redis cluster is not supported in Feast 0.2. Please set 'clusterEnabled: false' in sinkStorageSpec.options.");
        }
        JedisPool jedisPool = new JedisPool(sinkStorageSpec.getOptionsOrThrow("host"), redisPort);

        // Make sure we can connect to Redis according to the sinkStorageSpec
        try {
          jedisPool.getResource();
        } catch (JedisConnectionException e) {
          throw new RuntimeException(
              String.format(
                  "Failed to connect to Redis at host: '%s' port: '%d'. Please check the 'options' values in sinkStorageSpec.",
                  redisHost, redisPort));
        }
        jedisPool.close();
        break;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Unsupported type of sinkStorageSpec: \"%s\". Only REDIS and BIGQUERY are supported in Feast 0.2",
                storageSpecType));
    }
  }

  private static String generateName() {
    byte[] bytes = new byte[7];
    random.nextBytes(bytes);
    String randomHex = DigestUtils.sha1Hex(bytes).substring(0, 7);
    return String.format("feast-importjob-%s-%s", DateTime.now().getMillis(), randomHex);
  }

  public void expand() {
    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(
        TypeDescriptor.of(FeatureRow.class), ProtoCoder.of(FeatureRow.class));
    coderRegistry.registerCoderForType(
        TypeDescriptor.of(FeatureRowExtended.class), ProtoCoder.of(FeatureRowExtended.class));
    coderRegistry.registerCoderForType(TypeDescriptor.of(TableRow.class), TableRowJsonCoder.of());

    ImportJobPipelineOptions pipelineOptions =
        pipeline.getOptions().as(ImportJobPipelineOptions.class);

    PCollection<FeatureRow> features = pipeline.apply("Read", readFeaturesTransform);
    if (pipelineOptions.getSampleLimit() > 0) {
      features = features.apply(Sample.any(pipelineOptions.getSampleLimit()));
    }

    PCollection<FeatureRowExtended> featuresExtended =
        features.apply("Wrap with attempt data", new ToFeatureRowExtended());

    PFeatureRows pFeatureRows = PFeatureRows.of(featuresExtended);
    // pFeatureRows = pFeatureRows.applyDoFn("Convert feature types", new ConvertTypesDoFn(specs));
    // pFeatureRows = pFeatureRows.apply("Validate features", new ValidateTransform(specs));

    log.info(
        "A sample of size 1 of incoming rows from MAIN and ERRORS will logged every 30 seconds for visibility");
    logNRows(pFeatureRows, "Output sample", 1, Duration.standardSeconds(30));

    PCollection<FeatureRowExtended> featureRows = pFeatureRows.getMain();
    featureRows.apply(writeFeaturesTransform);

    PCollection<FeatureRowExtended> errorRows = pFeatureRows.getErrors();
    errorRows.apply(errorsStoreTransform);

    // if (!dryRun) {
    //   applySinkTransform(
    //       importJobSpecs.getSinkStorageSpec().getType(), pipelineOptions, featureRows);
    //
    //   errorRows.apply(errorsStoreTransform);
    // }
  }

  public PipelineResult run() {
    PipelineResult result = pipeline.run();
    log.info(String.format("FeastImportJobId:%s", this.retrieveId(result)));
    return result;
  }

  private PCollection<FeatureRowExtended> setupSink(PCollection<FeatureRowExtended> featureRows) {
    return featureRows;
  }

  private void logNRows(PFeatureRows pFeatureRows, String name, long limit, Duration period) {
    PCollection<FeatureRowExtended> main = pFeatureRows.getMain();
    PCollection<FeatureRowExtended> errors = pFeatureRows.getErrors();

    if (main.isBounded().equals(IsBounded.UNBOUNDED)) {
      Window<FeatureRowExtended> minuteWindow =
          Window.<FeatureRowExtended>into(FixedWindows.of(period))
              .triggering(AfterWatermark.pastEndOfWindow())
              .discardingFiredPanes()
              .withAllowedLateness(Duration.ZERO);
      main = main.apply(minuteWindow);
      errors = errors.apply(minuteWindow);
    }

    main.apply("metrics.store.lag", ParDo.of(FeastMetrics.lagUpdateDoFn()));

    main.apply("Sample success", Sample.any(limit))
        .apply("Log success sample", ParDo.of(new LoggerDoFn(Level.INFO, name + " MAIN ")));
    errors
        .apply("Sample errors", Sample.any(limit))
        .apply("Log errors sample", ParDo.of(new LoggerDoFn(Level.ERROR, name + " ERRORS ")));
  }

  private String retrieveId(PipelineResult result) {
    Class<? extends PipelineRunner<?>> runner = options.getRunner();
    if (runner.isAssignableFrom(DataflowRunner.class)) {
      return ((DataflowPipelineJob) result).getJobId();
    } else {
      return this.options.getJobName();
    }
  }

  // private PDone applySinkTransform(
  //     String sinkType,
  //     ImportJobPipelineOptions pipelineOptions,
  //     PCollection<FeatureRowExtended> featureRows) {
  //
  // switch (sinkType) {
  //   case RedisServingFactory.TYPE_REDIS:
  //     if (pipelineOptions.isCoalesceRowsEnabled()) {
  //       // Should we merge and dedupe rows before writing to the serving store?
  //       featureRows =
  //           featureRows.apply(
  //               "Coalesce Rows",
  //               new CoalesceFeatureRowExtended(
  //                   pipelineOptions.getCoalesceRowsDelaySeconds(),
  //                   pipelineOptions.getCoalesceRowsTimeoutSeconds()));
  //     }
  //     return featureRows.apply("Write to Serving Store", servingStoreTransform);
  //   case BigQueryWarehouseFactory.TYPE_BIGQUERY:
  //     return featureRows.apply("Write to Warehouse Store", warehouseStoreTransform);
  //   default:
  //     throw new UnsupportedStoreException(
  //         Strings.lenientFormat("Store of type %s is not supported by Feast.", sinkType));
  // }
  // }
}
