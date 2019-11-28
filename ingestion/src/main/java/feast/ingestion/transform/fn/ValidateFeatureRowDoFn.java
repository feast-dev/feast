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
import feast.ingestion.values.FailedElement;
import feast.ingestion.values.FailedElement.Builder;
import feast.ingestion.values.FeatureSetSpec;
import feast.ingestion.values.Field;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto;
import feast.types.ValueProto.Value.ValCase;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

@AutoValue
public abstract class ValidateFeatureRowDoFn extends DoFn<FeatureRow, FeatureRow> {

  public abstract Map<String, FeatureSetSpec> getFeatureSetSpecs();

  public abstract TupleTag<FeatureRow> getSuccessTag();

  public abstract TupleTag<FailedElement> getFailureTag();

  public static Builder newBuilder() {
    return new AutoValue_ValidateFeatureRowDoFn.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setFeatureSetSpecs(Map<String, FeatureSetSpec> featureSetSpecs);

    public abstract Builder setSuccessTag(TupleTag<FeatureRow> successTag);

    public abstract Builder setFailureTag(TupleTag<FailedElement> failureTag);

    public abstract ValidateFeatureRowDoFn build();
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    String error = null;
    FeatureRow featureRow = context.element();
    FeatureSetSpec featureSetSpec =
        getFeatureSetSpecs().getOrDefault(featureRow.getFeatureSet(), null);
    if (featureSetSpec != null) {

      for (FieldProto.Field field : featureRow.getFieldsList()) {
        Field fieldSpec = featureSetSpec.getField(field.getName());
        if (fieldSpec == null) {
          error =
              String.format(
                  "FeatureRow contains field '%s' which do not exists in FeatureSet '%s' version '%d'. Please check the FeatureRow data.",
                  field.getName(), featureSetSpec.getId());
          break;
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
      }
    } else {
      error =
          String.format(
              "FeatureRow contains invalid feature set id %s. Please check that the feature rows are being published to the correct topic on the feature stream.",
              featureRow.getFeatureSet());
    }

    if (error != null) {
      FailedElement.Builder failedElement =
          FailedElement.newBuilder()
              .setTransformName("ValidateFeatureRow")
              .setJobName(context.getPipelineOptions().getJobName())
              .setPayload(featureRow.toString())
              .setErrorMessage(error);
      if (featureSetSpec != null) {
        String[] split = featureSetSpec.getId().split(":");
        failedElement = failedElement.setFeatureSetName(split[0]).setFeatureSetVersion(split[1]);
      }
      context.output(getFailureTag(), failedElement.build());
    } else {
      context.output(getSuccessTag(), featureRow);
    }
  }
}
