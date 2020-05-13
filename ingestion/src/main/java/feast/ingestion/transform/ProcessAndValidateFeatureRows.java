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
package feast.ingestion.transform;

import com.google.auto.value.AutoValue;
import feast.core.FeatureSetProto;
import feast.ingestion.transform.fn.ProcessFeatureRowDoFn;
import feast.ingestion.transform.fn.ValidateFeatureRowDoFn;
import feast.ingestion.values.FeatureSet;
import feast.storage.api.writer.FailedElement;
import feast.types.FeatureRowProto.FeatureRow;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.commons.lang3.tuple.Pair;

@AutoValue
public abstract class ProcessAndValidateFeatureRows
    extends PTransform<PCollection<FeatureRow>, PCollectionTuple> {

  public abstract Map<String, FeatureSetProto.FeatureSetSpec> getFeatureSetSpecs();

  public abstract TupleTag<FeatureRow> getSuccessTag();

  public abstract TupleTag<FailedElement> getFailureTag();

  public static Builder newBuilder() {
    return new AutoValue_ProcessAndValidateFeatureRows.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setFeatureSetSpecs(
        Map<String, FeatureSetProto.FeatureSetSpec> featureSets);

    public abstract Builder setSuccessTag(TupleTag<FeatureRow> successTag);

    public abstract Builder setFailureTag(TupleTag<FailedElement> failureTag);

    public abstract ProcessAndValidateFeatureRows build();
  }

  @Override
  public PCollectionTuple expand(PCollection<FeatureRow> input) {

    Map<String, FeatureSet> featureSets =
        getFeatureSetSpecs().entrySet().stream()
            .map(e -> Pair.of(e.getKey(), new FeatureSet(e.getValue())))
            .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));

    return input
        .apply("ProcessFeatureRows", ParDo.of(new ProcessFeatureRowDoFn()))
        .apply(
            "ValidateFeatureRows",
            ParDo.of(
                    ValidateFeatureRowDoFn.newBuilder()
                        .setFeatureSets(featureSets)
                        .setSuccessTag(getSuccessTag())
                        .setFailureTag(getFailureTag())
                        .build())
                .withOutputTags(getSuccessTag(), TupleTagList.of(getFailureTag())));
  }
}
