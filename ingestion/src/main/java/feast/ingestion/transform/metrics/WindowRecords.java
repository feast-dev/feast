package feast.ingestion.transform.metrics;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

public class WindowRecords<T> extends
    PTransform<PCollection<T>, PCollection<KV<Integer, Iterable<T>>>> {

  private final long windowSize;

  public WindowRecords(long windowSize) {
    this.windowSize = windowSize;
  }

  @Override
  public PCollection<KV<Integer, Iterable<T>>> expand(PCollection<T> input) {
    return input
        .apply("Window records",
            Window.into(FixedWindows.of(Duration.standardSeconds(windowSize))))
        .apply("Add key", ParDo.of(new DoFn<T, KV<Integer, T>>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            c.output(KV.of(1, c.element()));
          }
        }))
        .apply("Collect", GroupByKey.create());
  }
}
