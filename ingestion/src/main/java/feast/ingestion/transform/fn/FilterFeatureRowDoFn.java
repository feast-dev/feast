package feast.ingestion.transform.fn;

import feast.types.FeatureProto.Feature;
import feast.types.FeatureRowProto.FeatureRow;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * Filter FeatureRow to only contain feature with given IDs
 */
public class FilterFeatureRowDoFn extends DoFn<FeatureRow, FeatureRow> {
  private final Set<String> featureIds;

  public FilterFeatureRowDoFn(List<String> featureIds) {
    this.featureIds = new HashSet<>(featureIds);
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    FeatureRow input = context.element();
    FeatureRow.Builder output = FeatureRow.newBuilder(input).clearFeatures();
    for (Feature feature : input.getFeaturesList()) {
      if (featureIds.contains(feature.getId())) {
        output.addFeatures(feature);
      }
    }
    context.output(output.build());
  }
}
