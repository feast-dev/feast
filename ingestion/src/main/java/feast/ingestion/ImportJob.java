package feast.ingestion;

import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.SourceProto.KafkaSourceConfig;
import feast.core.SourceProto.Source;
import feast.core.SourceProto.SourceType;
import feast.core.StoreProto.Store;
import feast.ingestion.options.ImportOptions;
import feast.ingestion.transform.ReadFromSource;
import feast.ingestion.transform.WriteFailedElementToBigQuery;
import feast.ingestion.transform.WriteToStore;
import feast.ingestion.utils.ResourceUtil;
import feast.ingestion.utils.SpecUtil;
import feast.ingestion.values.FailedElement;
import feast.types.FeatureRowProto.FeatureRow;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

public class ImportJob {
  // Tag for main output containing Feature Row that has been successfully processed.
  private static final TupleTag<FeatureRow> FEATURE_ROW_OUT = new TupleTag<FeatureRow>() {};
  // Tag for deadletter output containing elements and error messages from invalid input/transform.
  private static final TupleTag<FailedElement> DEADLETTER_OUT = new TupleTag<FailedElement>() {};

  public static void main(String[] args) throws InvalidProtocolBufferException {
    ImportOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().create().as(ImportOptions.class);
    run(options);

    // Source source =
    //     Source.newBuilder()
    //         .setType(SourceType.KAFKA)
    //         .setKafkaSourceConfig(
    //             KafkaSourceConfig.newBuilder()
    //                 .setBootstrapServers("localhost:9092")
    //                 .setTopic("mytopic")
    //                 .build())
    //         .build();
    //
    // FeatureSetSpec featureSetSpec = FeatureSetSpec.getDefaultInstance();
    // Store store = Store.getDefaultInstance();
    //
    // PCollectionTuple convertedFeatureRows =
    //     pipeline.apply(
    //         ReadFromSource.newBuilder()
    //             .setSource(source)
    //             .setSuccessTag(FEATURE_ROW_OUT)
    //             .setFailureTag(DEADLETTER_OUT)
    //             .build());
    //
    // convertedFeatureRows
    //     .get(FEATURE_ROW_OUT)
    //
    // .apply(WriteToStore.newBuilder().setFeatureSetSpec(featureSetSpec).setStore(store).build());
    //
    // if (options.getDeadLetterTableSpec() != null) {
    //   convertedFeatureRows
    //       .get(DEADLETTER_OUT)
    //       .apply(
    //           "WriteFailedElements",
    //           WriteFailedElementToBigQuery.newBuilder()
    //               .setJsonSchema(ResourceUtil.getDeadletterTableSchemaJson())
    //               .setTableSpec(options.getDeadLetterTableSpec())
    //               .build());
    // }

    // pipeline
    //     .apply(Create.of(Arrays.asList(1, 2)))
    //     .apply(new Foo());
    // .apply(
    //     WriteFailedElementToBigQuery.newBuilder()
    //         .setJsonSchema(ResourceUtil.getDeadletterTableSchemaJson())
    //         .setTableSpec("the-big-data-staging-007:dheryanto.feast_failed_elements")
    //         .build());

    // pipeline.run();

    // ImportOptions options =
    //     PipelineOptionsFactory.fromArgs(args).withValidation().as(ImportOptions.class);
    // run(options);
  }

  public static class Foo extends PTransform<PCollection<Integer>, PCollection<FailedElement>> {

    @Override
    public PCollection<FailedElement> expand(PCollection<Integer> input) {
      return input.apply(
          ParDo.of(
              new DoFn<Integer, FailedElement>() {
                @ProcessElement
                public void processElement(ProcessContext context) {
                  // FailedElement failedElement =
                  //     FailedElement.builder().errorMessage("errorMessage").build();
                  // System.out.println(failedElement);
                  // context.output(failedElement);
                }
              }));
    }
  }

  public static PipelineResult run(ImportOptions options) throws InvalidProtocolBufferException {
    PipelineOptionsValidator.validate(ImportOptions.class, options);
    Pipeline pipeline = Pipeline.create(options);

    List<FeatureSetSpec> featureSetSpecs =
        SpecUtil.parseFeatureSetSpecJsonList(options.getFeatureSetSpecJson());
    List<Store> stores = SpecUtil.parseStoreJsonList(options.getStoreJson());

    for (Store store : stores) {
      List<FeatureSetSpec> subscribedFeatureSets =
          SpecUtil.getSubscribedFeatureSets(store.getSubscriptionsList(), featureSetSpecs);

      for (FeatureSetSpec featureSet : subscribedFeatureSets) {
        PCollectionTuple convertedFeatureRows =
            pipeline.apply(
                ReadFromSource.newBuilder()
                    .setSource(featureSet.getSource())
                    .setSuccessTag(FEATURE_ROW_OUT)
                    .setFailureTag(DEADLETTER_OUT)
                    .build());

        convertedFeatureRows
            .get(FEATURE_ROW_OUT)
            .apply(WriteToStore.newBuilder().setFeatureSetSpec(featureSet).setStore(store).build());

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

  public static void writeToSingleStore(
      Pipeline pipeline, Store store, List<FeatureSetSpec> featureSetSpecs) {
    List<FeatureSetSpec> subscribedFeatureSets =
        SpecUtil.getSubscribedFeatureSets(store.getSubscriptionsList(), featureSetSpecs);
  }

  public static void writeToSingleStoreForSingleFeatureSet(
      Pipeline pipeline, Store store, FeatureSetSpec featureSet) {
    Source source = featureSet.getSource();
    if (!source.getType().equals(SourceType.KAFKA)) {
      throw new UnsupportedOperationException(
          "Only KAFKA source is currently supported. Please update the source in the FeatureSetSpec.");
    }

    KafkaSourceConfig config = source.getKafkaSourceConfig();
    // TODO: Generate and use unique step name

  }
}
