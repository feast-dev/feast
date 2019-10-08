package feast.ingestion;

import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.SourceProto.KafkaSourceConfig;
import feast.core.SourceProto.Source;
import feast.core.SourceProto.SourceType;
import feast.core.StoreProto.Store;
import feast.ingestion.options.ImportOptions;
import feast.ingestion.utils.SpecUtil;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class ImportJob {
  public static void main(String[] args) throws InvalidProtocolBufferException {
    ImportOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(ImportOptions.class);
    run(options);
  }

  public static PipelineResult run(ImportOptions options) throws InvalidProtocolBufferException {
    Pipeline pipeline = Pipeline.create(options);

    List<FeatureSetSpec> featureSetSpecs =
        SpecUtil.parseFeatureSetSpecJsonList(options.getFeatureSetSpecJson());
    List<Store> stores = SpecUtil.parseStoreJsonList(options.getStoreJson());

    for (int i = 0; i < stores.size(); i++) {
      Store store = stores.get(i);
      List<FeatureSetSpec> subscribedFeatureSets =
          SpecUtil.getSubscribedFeatureSets(store.getSubscriptionsList(), featureSetSpecs);
      for (FeatureSetSpec featureSet : subscribedFeatureSets) {
        String readFromSourceStepName =
            String.format("ReadFromSource_%s_%s_%s", featureSet.getName(), featureSet.getVersion(), i);

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
