package feast.ingestion;

import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.StoreProto.Store;
import feast.ingestion.options.ImportOptions;
import feast.ingestion.transform.ReadFromSource;
import feast.ingestion.transform.WriteFailedElementToBigQuery;
import feast.ingestion.transform.WriteToStore;
import feast.ingestion.utils.ResourceUtil;
import feast.ingestion.utils.SpecUtil;
import feast.ingestion.utils.StoreUtil;
import feast.ingestion.values.FailedElement;
import feast.types.FeatureRowProto.FeatureRow;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

@Slf4j
public class ImportJob {
  // Tag for main output containing Feature Row that has been successfully processed.
  private static final TupleTag<FeatureRow> FEATURE_ROW_OUT = new TupleTag<FeatureRow>() {};

  // Tag for deadletter output containing elements and error messages from invalid input/transform.
  private static final TupleTag<FailedElement> DEADLETTER_OUT = new TupleTag<FailedElement>() {};

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
     * 2. Write FeatureRow to the corresponding Store
     * 3. Write elements that failed to be processed to a dead letter queue.
     */

    PipelineOptionsValidator.validate(ImportOptions.class, options);
    Pipeline pipeline = Pipeline.create(options);

    List<FeatureSetSpec> featureSetSpecs =
        SpecUtil.parseFeatureSetSpecJsonList(options.getFeatureSetSpecJson());
    List<Store> stores = SpecUtil.parseStoreJsonList(options.getStoreJson());

    for (Store store : stores) {
      List<FeatureSetSpec> subscribedFeatureSets =
          SpecUtil.getSubscribedFeatureSets(store.getSubscriptionsList(), featureSetSpecs);

      for (FeatureSetSpec featureSet : subscribedFeatureSets) {
        // Ensure Store has valid configuration and Feast can access it.
        StoreUtil.setupStore(store, featureSet);

        // Step 1. Read messages from Feast Source as FeatureRow.
        PCollectionTuple convertedFeatureRows =
            pipeline.apply(
                "ReadFeatureRowFromSource",
                ReadFromSource.newBuilder()
                    .setSource(featureSet.getSource())
                    .setFieldByName(SpecUtil.getFieldByName(featureSet))
                    .setFeatureSetName(featureSet.getName())
                    .setFeatureSetVersion(featureSet.getVersion())
                    .setSuccessTag(FEATURE_ROW_OUT)
                    .setFailureTag(DEADLETTER_OUT)
                    .build());

        // Step 2. Write FeatureRow to the corresponding Store.
        convertedFeatureRows
            .get(FEATURE_ROW_OUT)
            .apply(
                "WriteFeatureRowToStore",
                WriteToStore.newBuilder().setFeatureSetSpec(featureSet).setStore(store).build());

        // Step 3. Write FailedElements to a dead letter table in BigQuery.
        if (options.getDeadLetterTableSpec() != null) {
          convertedFeatureRows
              .get(DEADLETTER_OUT)
              .apply(
                  "WriteFailedElements",
                  WriteFailedElementToBigQuery.newBuilder()
                      .setJsonSchema(ResourceUtil.getDeadletterTableSchemaJson())
                      .setTableSpec(options.getDeadLetterTableSpec())
                      .build());
        }
      }
    }

    return pipeline.run();
  }
}
