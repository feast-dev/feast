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

import static feast.ingestion.utils.StoreUtil.getFeatureSink;

import com.google.protobuf.InvalidProtocolBufferException;
import feast.common.models.FeatureSetReference;
import feast.ingestion.options.ImportOptions;
import feast.ingestion.transform.FeatureRowToStoreAllocator;
import feast.ingestion.transform.ProcessAndValidateFeatureRows;
import feast.ingestion.transform.ReadFromSource;
import feast.ingestion.transform.metrics.WriteFailureMetricsTransform;
import feast.ingestion.transform.metrics.WriteInflightMetricsTransform;
import feast.ingestion.transform.metrics.WriteSuccessMetricsTransform;
import feast.ingestion.transform.specs.ReadFeatureSetSpecs;
import feast.ingestion.transform.specs.WriteFeatureSetSpecAck;
import feast.ingestion.utils.SpecUtil;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.IngestionJobProto.SpecsStreamingUpdateConfig;
import feast.proto.core.SourceProto.Source;
import feast.proto.core.StoreProto.Store;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.storage.api.writer.DeadletterSink;
import feast.storage.api.writer.FailedElement;
import feast.storage.api.writer.FeatureSink;
import feast.storage.api.writer.WriteResult;
import feast.storage.connectors.bigquery.writer.BigQueryDeadletterSink;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.commons.lang3.tuple.Pair;
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
     * 1. Read FeatureSetSpec messages from kafka
     * 2. Read messages from Feast Source as FeatureRow
     * 3. Validate the feature rows to ensure the schema matches what is registered to the system
     * 4. Distribute rows across stores by subscription
     * 5. Write FeatureRow to the corresponding Store
     * 6. Write elements that failed to be processed to a dead letter queue.
     * 7. Write metrics to a metrics sink
     * 8. Send ack on receiving FeatureSetSpec
     */

    PipelineOptionsValidator.validate(ImportOptions.class, options);
    Pipeline pipeline = Pipeline.create(options);

    log.info("Starting import job with settings: \n{}", options.toString());

    List<Store> stores = SpecUtil.parseStoreJsonList(options.getStoresJson());
    Source source = SpecUtil.parseSourceJson(options.getSourceJson());
    SpecsStreamingUpdateConfig specsStreamingUpdateConfig =
        SpecUtil.parseSpecsStreamingUpdateConfig(options.getSpecsStreamingUpdateConfigJson());

    // Step 1. Read FeatureSetSpecs from Spec source
    PCollection<KV<FeatureSetReference, FeatureSetSpec>> featureSetSpecs =
        pipeline.apply(
            "ReadFeatureSetSpecs",
            ReadFeatureSetSpecs.newBuilder()
                .setSource(source)
                .setStores(stores)
                .setSpecsStreamingUpdateConfig(specsStreamingUpdateConfig)
                .build());

    PCollectionView<Map<String, Iterable<FeatureSetSpec>>> globalSpecView =
        featureSetSpecs
            .apply(MapElements.via(new ReferenceToString()))
            .apply("GlobalSpecView", View.asMultimap());

    // Step 2. Read messages from Feast Source as FeatureRow.
    PCollectionTuple convertedFeatureRows =
        pipeline.apply(
            "ReadFeatureRowFromSource",
            ReadFromSource.newBuilder()
                .setSource(source)
                .setSuccessTag(FEATURE_ROW_OUT)
                .setFailureTag(DEADLETTER_OUT)
                .build());

    // Step 3. Process and validate incoming FeatureRows
    PCollectionTuple validatedRows =
        convertedFeatureRows
            .get(FEATURE_ROW_OUT)
            .apply(
                ProcessAndValidateFeatureRows.newBuilder()
                    .setDefaultProject(options.getDefaultFeastProject())
                    .setFeatureSetSpecs(globalSpecView)
                    .setSuccessTag(FEATURE_ROW_OUT)
                    .setFailureTag(DEADLETTER_OUT)
                    .build());

    Map<Store, TupleTag<FeatureRow>> storeTags =
        stores.stream()
            .map(s -> Pair.of(s, new TupleTag<FeatureRow>()))
            .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));

    // Step 4. Allocate validated rows to stores by store subscription
    PCollectionTuple storeAllocatedRows =
        validatedRows
            .get(FEATURE_ROW_OUT)
            .apply(
                FeatureRowToStoreAllocator.newBuilder()
                    .setStores(stores)
                    .setStoreTags(storeTags)
                    .build());

    PCollectionList<FeatureSetReference> sinkReadiness = PCollectionList.empty(pipeline);

    for (Store store : stores) {
      FeatureSink featureSink = getFeatureSink(store);

      sinkReadiness = sinkReadiness.and(featureSink.prepareWrite(featureSetSpecs));
      PCollection<FeatureRow> rowsForStore =
          storeAllocatedRows.get(storeTags.get(store)).setCoder(ProtoCoder.of(FeatureRow.class));

      // Step 5. Write metrics of successfully validated rows
      rowsForStore.apply(
          "WriteInflightMetrics", WriteInflightMetricsTransform.create(store.getName()));

      // Step 6. Write FeatureRow to the corresponding Store.
      WriteResult writeFeatureRows =
          rowsForStore.apply("WriteFeatureRowToStore", featureSink.writer());

      // Step 7. Write FailedElements to a dead letter table in BigQuery.
      if (options.getDeadLetterTableSpec() != null) {
        // TODO: make deadletter destination type configurable
        DeadletterSink deadletterSink =
            new BigQueryDeadletterSink(options.getDeadLetterTableSpec());

        convertedFeatureRows
            .get(DEADLETTER_OUT)
            .apply("WriteFailedElements_ReadFromSource", deadletterSink.write());

        validatedRows
            .get(DEADLETTER_OUT)
            .apply("WriteFailedElements_ValidateRows", deadletterSink.write());

        writeFeatureRows
            .getFailedInserts()
            .apply("WriteFailedElements_WriteFeatureRowToStore", deadletterSink.write());
      }

      // Step 8. Write metrics to a metrics sink.
      writeFeatureRows
          .getSuccessfulInserts()
          .apply("WriteSuccessMetrics", WriteSuccessMetricsTransform.create(store.getName()));

      writeFeatureRows
          .getFailedInserts()
          .apply("WriteFailureMetrics", WriteFailureMetricsTransform.create(store.getName()));
    }

    sinkReadiness
        .apply(Flatten.pCollections())
        .apply(
            "WriteAck",
            WriteFeatureSetSpecAck.newBuilder()
                .setSinksCount(stores.size())
                .setSpecsStreamingUpdateConfig(specsStreamingUpdateConfig)
                .build());

    return pipeline.run();
  }

  private static class ReferenceToString
      extends SimpleFunction<KV<FeatureSetReference, FeatureSetSpec>, KV<String, FeatureSetSpec>> {
    public KV<String, FeatureSetSpec> apply(KV<FeatureSetReference, FeatureSetSpec> input) {
      return KV.of(input.getKey().getReference(), input.getValue());
    }
  }
}
