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
import feast.ingestion.transform.fn.ProcessFeatureRowDoFn;
import feast.ingestion.transform.fn.ValidateFeatureRowDoFn;
import feast.proto.core.FeatureSetProto;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.storage.api.writer.FailedElement;
import java.util.Map;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;

@AutoValue
public abstract class ProcessAndValidateFeatureRows
    extends PTransform<PCollection<FeatureRow>, PCollectionTuple> {

  public abstract PCollectionView<Map<String, Iterable<FeatureSetProto.FeatureSetSpec>>>
      getFeatureSetSpecs();

  public abstract String getDefaultProject();

  public abstract TupleTag<FeatureRow> getSuccessTag();

  public abstract TupleTag<FailedElement> getFailureTag();

  public static Builder newBuilder() {
    return new AutoValue_ProcessAndValidateFeatureRows.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setFeatureSetSpecs(
        PCollectionView<Map<String, Iterable<FeatureSetProto.FeatureSetSpec>>> featureSets);

    public abstract Builder setDefaultProject(String defaultProject);

    public abstract Builder setSuccessTag(TupleTag<FeatureRow> successTag);

    public abstract Builder setFailureTag(TupleTag<FailedElement> failureTag);

    public abstract ProcessAndValidateFeatureRows build();
  }

  @Override
  public PCollectionTuple expand(PCollection<FeatureRow> input) {
    return input
        .apply("ProcessFeatureRows", ParDo.of(new ProcessFeatureRowDoFn(getDefaultProject())))
        .apply(
            "ValidateFeatureRows",
            ParDo.of(
                    ValidateFeatureRowDoFn.newBuilder()
                        .setFeatureSets(getFeatureSetSpecs())
                        .setSuccessTag(getSuccessTag())
                        .setFailureTag(getFailureTag())
                        .build())
                .withSideInputs(getFeatureSetSpecs())
                .withOutputTags(getSuccessTag(), TupleTagList.of(getFailureTag())));
  }
}
