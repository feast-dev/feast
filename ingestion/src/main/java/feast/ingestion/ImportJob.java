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
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import feast.ingestion.boot.ImportJobModule;
import feast.ingestion.boot.PipelineModule;
import feast.ingestion.config.ImportSpecSupplier;
import feast.ingestion.model.Specs;
import feast.ingestion.options.ImportJobPipelineOptions;
import feast.ingestion.options.JobOptions;
import feast.ingestion.transform.CoalescePFeatureRows;
import feast.ingestion.transform.ErrorsStoreTransform;
import feast.ingestion.transform.ReadFeaturesTransform;
import feast.ingestion.transform.ServingStoreTransform;
import feast.ingestion.transform.ToFeatureRowExtended;
import feast.ingestion.transform.ValidateTransform;
import feast.ingestion.transform.WarehouseStoreTransform;
import feast.ingestion.transform.fn.ConvertTypesDoFn;
import feast.ingestion.transform.fn.LoggerDoFn;
import feast.ingestion.transform.fn.RoundEventTimestampsDoFn;
import feast.ingestion.values.PFeatureRows;
import feast.options.OptionsParser;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import feast.types.FeatureRowProto.FeatureRow;
import java.util.Arrays;
import java.util.Random;
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

@Slf4j
public class ImportJob {

  private static Random random = new Random(System.currentTimeMillis());

  private final Pipeline pipeline;
  private final ImportSpec importSpec;
  private final ReadFeaturesTransform readFeaturesTransform;
  private final ServingStoreTransform servingStoreTransform;
  private final WarehouseStoreTransform warehouseStoreTransform;
  private final ErrorsStoreTransform errorsStoreTransform;
  private final boolean dryRun;
  private final ImportJobPipelineOptions options;
  private final Specs specs;

  @Inject
  public ImportJob(
      Pipeline pipeline,
      ImportSpec importSpec,
      ReadFeaturesTransform readFeaturesTransform,
      ServingStoreTransform servingStoreTransform,
      WarehouseStoreTransform warehouseStoreTransform,
      ErrorsStoreTransform errorsStoreTransform,
      ImportJobPipelineOptions options,
      Specs specs) {
    this.pipeline = pipeline;
    this.importSpec = importSpec;
    this.readFeaturesTransform = readFeaturesTransform;
    this.servingStoreTransform = servingStoreTransform;
    this.warehouseStoreTransform = warehouseStoreTransform;
    this.errorsStoreTransform = errorsStoreTransform;
    this.dryRun = options.isDryRun();
    this.options = options;
    this.specs = specs;
  }

  public static void main(String[] args) {
    mainWithResult(args);
  }

  public static PipelineResult mainWithResult(String[] args) {
    log.info("Arguments: " + Arrays.toString(args));
    ImportJobPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(ImportJobPipelineOptions.class);
    if (options.getJobName().isEmpty()) {
      options.setJobName(generateName());
    }
    log.info("options: " + options.toString());
    ImportSpec importSpec = new ImportSpecSupplier(options).get();
    Injector injector =
        Guice.createInjector(new ImportJobModule(options, importSpec), new PipelineModule());
    ImportJob job = injector.getInstance(ImportJob.class);

    job.expand();
    return job.run();
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

    JobOptions jobOptions = OptionsParser.parse(importSpec.getJobOptionsMap(), JobOptions.class);

    try {
      log.info(JsonFormat.printer().print(importSpec));
    } catch (InvalidProtocolBufferException e) {
      // pass
    }
    specs.validate();

    PCollection<FeatureRow> features = pipeline.apply("Read", readFeaturesTransform);
    if (jobOptions.getSampleLimit() > 0) {
      features = features.apply(Sample.any(jobOptions.getSampleLimit()));
    }

    PCollection<FeatureRowExtended> featuresExtended =
        features.apply("Wrap with attempt data", new ToFeatureRowExtended());

    PFeatureRows pFeatureRows = PFeatureRows.of(featuresExtended);
    pFeatureRows = pFeatureRows.applyDoFn("Convert feature types", new ConvertTypesDoFn(specs));
    pFeatureRows = pFeatureRows.apply("Validate features", new ValidateTransform(specs));

    log.info(
        "A sample of size 1 of incoming rows from MAIN and ERRORS will logged every 30 seconds for visibility");
    logNRows(pFeatureRows, "Output sample", 1, Duration.standardSeconds(30));

    PFeatureRows warehouseRows = roundTimestamps("Round timestamps for warehouse", pFeatureRows);
    PFeatureRows servingRows = pFeatureRows;
    if (jobOptions.isCoalesceRowsEnabled()) {
      // Should we merge and dedupe rows before writing to the serving store?
      servingRows = servingRows.apply("Coalesce Rows", new CoalescePFeatureRows(
          jobOptions.getCoalesceRowsDelaySeconds(),
          jobOptions.getCoalesceRowsTimeoutSeconds()));
    }
    servingRows = roundTimestamps("Round timestamps for serving", servingRows);

    if (!dryRun) {
      servingRows.apply("Write to Serving Stores", servingStoreTransform);
      warehouseRows.apply("Write to Warehouse  Stores", warehouseStoreTransform);
      pFeatureRows.getErrors().apply("Write errors", errorsStoreTransform);
    }
  }

  public PFeatureRows roundTimestamps(String name, PFeatureRows pFeatureRows) {
    return
        PFeatureRows.of(
            pFeatureRows
                .getMain()
                .apply(name,
                    ParDo.of(new RoundEventTimestampsDoFn())),
            pFeatureRows.getErrors());
  }

  public PipelineResult run() {
    PipelineResult result = pipeline.run();
    log.info(String.format("FeastImportJobId:%s", this.retrieveId(result)));
    return result;
  }

  public void logNRows(PFeatureRows pFeatureRows, String name, long limit, Duration period) {
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
}
