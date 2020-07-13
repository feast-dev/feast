/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
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

import com.google.protobuf.InvalidProtocolBufferException;
import feast.ingestion.enums.ValidationStatus;
import feast.ingestion.values.FeatureSet;
import feast.ingestion.values.Field;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FieldProto;
import feast.proto.types.ValueProto.Value.ValCase;
import feast.spark.ingestion.RowWithValidationResult;
import feast.storage.api.writer.FailedElement;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ValidateFeatureRowDoFn implements Serializable {

  private final HashMap<String, FeatureSet> featureSets;

  public ValidateFeatureRowDoFn(HashMap<String, FeatureSet> featureSets) {
    this.featureSets = featureSets;
  }

  public RowWithValidationResult validateElement(FeatureRow featureRow)
      throws InvalidProtocolBufferException {
    // TODO: Abstract duplicated validation logic into shared module.
    String error = null;
    byte[] featureRowBytes = featureRow.toByteArray();
    FeatureSet featureSet = featureSets.get(featureRow.getFeatureSet());
    List<FieldProto.Field> fields = new ArrayList<>();
    if (featureSet != null) {
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
              .setJobName(featureRow.getIngestionId())
              .setPayload(featureRow.toString())
              .setErrorMessage(error);

      if (featureSet != null) {
        String[] split = featureSet.getReference().split("/");
        failedElement = failedElement.setProjectName(split[0]).setFeatureSetName(split[1]);
      }

      return RowWithValidationResult.newBuilder()
          .setFeatureRow(featureRowBytes)
          .setValidationStatus(ValidationStatus.FAILURE)
          .setFailedElement(failedElement.build())
          .build();
    }
    return RowWithValidationResult.newBuilder()
        .setFeatureRow(featureRowBytes)
        .setValidationStatus(ValidationStatus.SUCCESS)
        .build();
  }
}
