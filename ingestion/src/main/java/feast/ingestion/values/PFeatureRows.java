/*
 * Copyright 2018 The Feast Authors
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
 *
 */
package feast.ingestion.values;

import feast.ingestion.transform.fn.BaseFeatureDoFn;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.MultiOutput;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

@AllArgsConstructor
@Value
@Slf4j
public class PFeatureRows implements PInput, POutput {

  public static final TupleTag<FeatureRowExtended> MAIN_TAG = new TupleTag<>();
  public static final TupleTag<FeatureRowExtended> ERRORS_TAG = new TupleTag<>();
  private static final AtomicInteger counter = new AtomicInteger();
  private PCollection<FeatureRowExtended> main;
  private PCollection<FeatureRowExtended> errors;

  public static PFeatureRows of(PCollection<FeatureRowExtended> input) {
    Pipeline pipeline = input.getPipeline();
    Create.Values<FeatureRowExtended> empty = Create.empty(ProtoCoder.of(FeatureRowExtended.class));
    return new PFeatureRows(input,
        pipeline.apply(input.getName() + "/empty.errors" + counter.incrementAndGet(), empty));
  }

  public static PFeatureRows of(
      PCollection<FeatureRowExtended> input, PCollection<FeatureRowExtended> errors) {
    return new PFeatureRows(input, errors);
  }

  @Override
  public Pipeline getPipeline() {
    return main.getPipeline();
  }

  @Override
  public Map<TupleTag<?>, PValue> expand() {
    Map<TupleTag<?>, PValue> expanded = new HashMap<>();
    expanded.put(MAIN_TAG, main);
    expanded.put(ERRORS_TAG, errors);
    return expanded;
  }

  @Override
  public void finishSpecifyingOutput(
      String transformName, PInput input, PTransform<?, ?> transform) {
  }

  /**
   * @return new PFeatureRows, which has any tagged errors and retries in DoFn added to the errors
   * and retries gathered so far.
   */
  public PFeatureRows applyDoFn(String name, BaseFeatureDoFn doFn) {
    MultiOutput<FeatureRowExtended, FeatureRowExtended> transform =
        ParDo.of(doFn.withTransformName(name))
            .withOutputTags(MAIN_TAG, TupleTagList.of(ERRORS_TAG));

    PCollectionTuple transformed = Pipeline.applyTransform(name, main, transform);

    PCollection<FeatureRowExtended> outMain =
        transformed.get(MAIN_TAG).setCoder(ProtoCoder.of(FeatureRowExtended.class));

    PCollection<FeatureRowExtended> outErrors =
        PCollectionList.of(
            transformed.get(ERRORS_TAG).setCoder(ProtoCoder.of(FeatureRowExtended.class)))
            .and(errors)
            .apply(name + "/Flatten errors", Flatten.pCollections())
            .setCoder(ProtoCoder.of(FeatureRowExtended.class));
    return new PFeatureRows(outMain, outErrors);
  }

  public PFeatureRows apply(String name, PTransform<PFeatureRows, PFeatureRows> transform) {
    return Pipeline.applyTransform(name, this, transform);
  }
}
