package feast.ingestion.transform;

import feast.ingestion.values.FailsafeFeatureRow;
import feast.types.FeatureRowProto.FeatureRow;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.TupleTag;

public class ReadFromSource extends PTransform<PInput, PCollectionTuple> {
  TupleTag<FailsafeFeatureRow<KV<byte[], byte[]>, FeatureRow>> successTag;
  TupleTag<FailsafeFeatureRow<KV<byte[], byte[]>, FeatureRow>> failureTag;

  public ReadFromSource withSuccessTag(
      TupleTag<FailsafeFeatureRow<KV<byte[], byte[]>, FeatureRow>> successTag) {
    this.successTag = successTag;
    return this;
  }

  public ReadFromSource withFailureTag(
      TupleTag<FailsafeFeatureRow<KV<byte[], byte[]>, FeatureRow>> failureTag) {
    this.failureTag = failureTag;
    return this;
  }

  @Override
  public PCollectionTuple expand(PInput input) {
    return null;
  }
}
