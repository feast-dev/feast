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
package feast.ingestion.transform.fn;

import com.google.auto.value.AutoValue;
import com.google.common.collect.Iterators;
import feast.ingestion.values.FeatureSet;
import feast.ingestion.values.Field;
import feast.proto.core.FeatureSetProto;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FieldProto;
import feast.proto.types.ValueProto.Value.ValCase;
import feast.storage.api.writer.FailedElement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class ValidateFeatureRowDoFn extends DoFn<FeatureRow, FeatureRow> {
  private static final Logger log = LoggerFactory.getLogger(ValidateFeatureRowDoFn.class);

  public abstract PCollectionView<Map<String, Iterable<FeatureSetProto.FeatureSetSpec>>>
      getFeatureSets();

  public abstract TupleTag<FeatureRow> getSuccessTag();

  public abstract TupleTag<FailedElement> getFailureTag();

  public static Builder newBuilder() {
    return new AutoValue_ValidateFeatureRowDoFn.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setFeatureSets(
        PCollectionView<Map<String, Iterable<FeatureSetProto.FeatureSetSpec>>> featureSets);

    public abstract Builder setSuccessTag(TupleTag<FeatureRow> successTag);

    public abstract Builder setFailureTag(TupleTag<FailedElement> failureTag);

    public abstract ValidateFeatureRowDoFn build();
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    String error = null;
    FeatureRow featureRow = context.element();
    Iterable<FeatureSetProto.FeatureSetSpec> featureSetSpecs =
        context.sideInput(getFeatureSets()).get(featureRow.getFeatureSet());
    if (featureSetSpecs == null) {
      log.warn(
          String.format(
              "FeatureRow contains invalid featureSetReference %s."
                  + " Please check that the feature rows are being published"
                  + " to the correct topic on the feature stream.",
              featureRow.getFeatureSet()));
      return;
    }

    List<FieldProto.Field> fields = new ArrayList<>();

    FeatureSetProto.FeatureSetSpec latestSpec = Iterators.getLast(featureSetSpecs.iterator());
    FeatureSet featureSet = new FeatureSet(latestSpec);

    for (FieldProto.Field field : featureRow.getFieldsList()) {
      Field fieldSpec = featureSet.getField(field.getName());
      if (fieldSpec == null) {
        // skip
        continue;
      }
      // If value is set in the FeatureRow, make sure the value type matches
      // that defined in FeatureSetSpec
      if (!field.getValue().getValCase().equals(ValCase.VAL_NOT_SET)) {
        int expectedTypeFieldNumber = fieldSpec.getType().getNumber();
        int actualTypeFieldNumber = field.getValue().getValCase().getNumber();
        if (expectedTypeFieldNumber != actualTypeFieldNumber) {
          error =
              String.format(
                  "FeatureRow contains field '%s' with invalid type '%s'. Feast expects the field type to match that in FeatureSet '%s'. Please check the FeatureRow data.",
                  field.getName(), field.getValue().getValCase(), fieldSpec.getType());
          break;
        }
      }
      if (!fields.contains(field)) {
        fields.add(field);
      }
    }

    if (error != null) {
      FailedElement.Builder failedElement =
          FailedElement.newBuilder()
              .setTransformName("ValidateFeatureRow")
              .setJobName(context.getPipelineOptions().getJobName())
              .setPayload(featureRow.toString())
              .setErrorMessage(error);
      if (featureSetSpecs != null) {
        FeatureSetProto.FeatureSetSpec spec = Iterators.getLast(featureSetSpecs.iterator());
        failedElement =
            failedElement.setProjectName(spec.getProject()).setFeatureSetName(spec.getName());
      }
      context.output(getFailureTag(), failedElement.build());
    } else {
      featureRow = featureRow.toBuilder().clearFields().addAllFields(fields).build();
      context.output(getSuccessTag(), featureRow);
    }
  }
}
