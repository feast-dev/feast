/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
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
package feast.ingestion;

import static feast.ingestion.utils.SpecUtil.getFeatureSetReference;
import static feast.ingestion.utils.StoreUtil.getFeatureSink;

import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.FeatureSetProto.FeatureSet;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.SourceProto.Source;
import feast.core.StoreProto.Store;
import feast.ingestion.options.BZip2Decompressor;
import feast.ingestion.options.ImportOptions;
import feast.ingestion.options.StringListStreamConverter;
import feast.ingestion.transform.ReadFromSource;
import feast.ingestion.transform.ValidateFeatureRows;
import feast.ingestion.transform.WriteFailedElementToBigQuery;
import feast.ingestion.transform.metrics.WriteFailureMetricsTransform;
import feast.ingestion.transform.metrics.WriteInflightMetricsTransform;
import feast.ingestion.transform.metrics.WriteSuccessMetricsTransform;
import feast.ingestion.utils.ResourceUtil;
import feast.ingestion.utils.SpecUtil;
import feast.storage.api.write.FailedElement;
import feast.storage.api.write.FeatureSink;
import feast.storage.api.write.WriteResult;
import feast.types.FeatureRowProto.FeatureRow;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;

public class ImportJob {

  // Tag for main output containing Feature Row that has been successfully processed.
  private static final TupleTag<FeatureRow> FEATURE_ROW_OUT = new TupleTag<FeatureRow>() {};

  // Tag for deadletter output containing elements and error messages from invalid input/transform.
  private static final TupleTag<FailedElement> DEADLETTER_OUT = new TupleTag<FailedElement>() {};
  private static final Logger log = org.slf4j.LoggerFactory.getLogger(ImportJob.class);

  /**
   * @param args arguments to be passed to Beam pipeline
   * @throws InvalidProtocolBufferException if options passed to the pipeline are invalid
   */
  public static void main(String[] args) throws IOException {
    ImportOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().create().as(ImportOptions.class);
    runPipeline(options);
  }

  @SuppressWarnings("UnusedReturnValue")
  public static PipelineResult runPipeline(ImportOptions options) throws IOException {
    /*
     * Steps:
     * 1. Read messages from Feast Source as FeatureRow
     * 2. Validate the feature rows to ensure the schema matches what is registered to the system
     * 3. Write FeatureRow to the corresponding Store
     * 4. Write elements that failed to be processed to a dead letter queue.
     * 5. Write metrics to a metrics sink
     */

    PipelineOptionsValidator.validate(ImportOptions.class, options);
    Pipeline pipeline = Pipeline.create(options);

    log.info("Starting import job with settings: \n{}", options.toString());

    BZip2Decompressor<List<String>> decompressor =
        new BZip2Decompressor<>(new StringListStreamConverter());
    List<String> featureSetJson = decompressor.decompress(options.getFeatureSetJson());
    List<FeatureSet> featureSets = SpecUtil.parseFeatureSetSpecJsonList(featureSetJson);
    List<Store> stores = SpecUtil.parseStoreJsonList(options.getStoreJson());

    for (Store store : stores) {
      List<FeatureSet> subscribedFeatureSets =
          SpecUtil.getSubscribedFeatureSets(store.getSubscriptionsList(), featureSets);

      // Generate tags by key
      Map<String, FeatureSetSpec> featureSetSpecsByKey = new HashMap<>();
      subscribedFeatureSets.stream()
          .forEach(
              fs -> {
                String ref = getFeatureSetReference(fs.getSpec());
                featureSetSpecsByKey.put(ref, fs.getSpec());
              });

      FeatureSink featureSink = getFeatureSink(store, featureSetSpecsByKey);

      // TODO: make the source part of the job initialisation options
      Source source = subscribedFeatureSets.get(0).getSpec().getSource();

      for (FeatureSet featureSet : subscribedFeatureSets) {
        // Ensure Store has valid configuration and Feast can access it.
        featureSink.prepareWrite(featureSet);
      }

      // Step 1. Read messages from Feast Source as FeatureRow.
      PCollectionTuple convertedFeatureRows =
          pipeline.apply(
              "ReadFeatureRowFromSource",
              ReadFromSource.newBuilder()
                  .setSource(source)
                  .setSuccessTag(FEATURE_ROW_OUT)
                  .setFailureTag(DEADLETTER_OUT)
                  .build());

      // Step 2. Validate incoming FeatureRows
      PCollectionTuple validatedRows =
          convertedFeatureRows
              .get(FEATURE_ROW_OUT)
              .apply(
                  ValidateFeatureRows.newBuilder()
                      .setFeatureSetSpecs(featureSetSpecsByKey)
                      .setSuccessTag(FEATURE_ROW_OUT)
                      .setFailureTag(DEADLETTER_OUT)
                      .build());

      validatedRows
          .get(FEATURE_ROW_OUT)
          .apply("WriteInflightMetrics", WriteInflightMetricsTransform.create(store.getName()));

      // Step 3. Write FeatureRow to the corresponding Store.
      WriteResult writeFeatureRows =
          validatedRows.get(FEATURE_ROW_OUT).apply("WriteFeatureRowToStore", featureSink.write());

      // Step 4. Write FailedElements to a dead letter table in BigQuery.
      if (options.getDeadLetterTableSpec() != null) {
        convertedFeatureRows
            .get(DEADLETTER_OUT)
            .apply(
                "WriteFailedElements_ReadFromSource",
                WriteFailedElementToBigQuery.newBuilder()
                    .setJsonSchema(ResourceUtil.getDeadletterTableSchemaJson())
                    .setTableSpec(options.getDeadLetterTableSpec())
                    .build());

        validatedRows
            .get(DEADLETTER_OUT)
            .apply(
                "WriteFailedElements_ValidateRows",
                WriteFailedElementToBigQuery.newBuilder()
                    .setJsonSchema(ResourceUtil.getDeadletterTableSchemaJson())
                    .setTableSpec(options.getDeadLetterTableSpec())
                    .build());

        writeFeatureRows
            .getFailedInserts()
            .apply(
                "WriteFailedElements_WriteFeatureRowToStore",
                WriteFailedElementToBigQuery.newBuilder()
                    .setJsonSchema(ResourceUtil.getDeadletterTableSchemaJson())
                    .setTableSpec(options.getDeadLetterTableSpec())
                    .build());
      }

      // Step 5. Write metrics to a metrics sink.
      writeFeatureRows
          .getSuccessfulInserts()
          .apply("WriteSuccessMetrics", WriteSuccessMetricsTransform.create(store.getName()));

      writeFeatureRows
          .getFailedInserts()
          .apply("WriteFailureMetrics", WriteFailureMetricsTransform.create(store.getName()));
    }

    return pipeline.run();
  }
}
