package feast.source;

import feast.types.FeatureRowProto.FeatureRow;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;

public abstract class FeatureSource extends PTransform<PInput, PCollection<FeatureRow>> {

}
