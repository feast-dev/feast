package feast.ingestion;

import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.SourceProto.Source;
import feast.core.StoreProto.Store;
import feast.ingestion.options.ImportOptions;
import feast.ingestion.transform.ReadFromSource;
import feast.ingestion.transform.ValidateFeatureRows;
import feast.ingestion.transform.WriteFailedElementToBigQuery;
import feast.ingestion.transform.WriteToStore;
import feast.ingestion.transform.metrics.WriteMetricsTransform;
import feast.ingestion.utils.ResourceUtil;
import feast.ingestion.utils.SpecUtil;
import feast.ingestion.utils.StoreUtil;
import feast.ingestion.values.FailedElement;
import feast.types.FeatureRowProto.FeatureRow;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
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
  public static void main(String[] args) throws InvalidProtocolBufferException {
    ImportOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().create().as(ImportOptions.class);
    runPipeline(options);
  }

  @SuppressWarnings("UnusedReturnValue")
  public static PipelineResult runPipeline(ImportOptions options)
      throws InvalidProtocolBufferException {
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

    List<FeatureSetSpec> featureSetSpecs =
        SpecUtil.parseFeatureSetSpecJsonList(options.getFeatureSetSpecJson());
    List<Store> stores = SpecUtil.parseStoreJsonList(options.getStoreJson());

    for (Store store : stores) {
      List<FeatureSetSpec> subscribedFeatureSets =
          SpecUtil.getSubscribedFeatureSets(store.getSubscriptionsList(), featureSetSpecs);

      // Generate tags by key
      Map<String, FeatureSetSpec> featureSetSpecsByKey =
          subscribedFeatureSets.stream()
              .map(
                  fs -> {
                    String id = String.format("%s:%s", fs.getName(), fs.getVersion());
                    return Pair.of(id, fs);
                  })
              .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));

      // TODO: make the source part of the job initialisation options
      Source source = subscribedFeatureSets.get(0).getSource();

      // Step 1. Read messages from Feast Source as FeatureRow.
      PCollectionTuple convertedFeatureRows =
          pipeline.apply(
              "ReadFeatureRowFromSource",
              ReadFromSource.newBuilder()
                  .setSource(source)
                  .setSuccessTag(FEATURE_ROW_OUT)
                  .setFailureTag(DEADLETTER_OUT)
                  .build());

      for (FeatureSetSpec featureSet : subscribedFeatureSets) {
        // Ensure Store has valid configuration and Feast can access it.
        StoreUtil.setupStore(store, featureSet);
      }

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

      // Step 3. Write FeatureRow to the corresponding Store.
      validatedRows
          .get(FEATURE_ROW_OUT)
          .apply(
              "WriteFeatureRowToStore",
              WriteToStore.newBuilder()
                  .setFeatureSetSpecs(featureSetSpecsByKey)
                  .setStore(store)
                  .build());

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
      }

      // Step 5. Write metrics to a metrics sink.
      validatedRows.apply(
          "WriteMetrics",
          WriteMetricsTransform.newBuilder()
              .setStoreName(store.getName())
              .setSuccessTag(FEATURE_ROW_OUT)
              .setFailureTag(DEADLETTER_OUT)
              .build());
    }

    return pipeline.run();
  }
}
