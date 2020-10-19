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
package feast.spark.ingestion;

import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FieldProto;
import feast.proto.types.FieldProto.Field;
import feast.proto.types.ValueProto;
import feast.spark.ingestion.common.FailedElement;
import feast.spark.ingestion.common.FeatureSet;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class FeatureRowValidator implements Serializable {

  private final String defaultProject;

  public FeatureRowValidator(String defaultProject) {
    this.defaultProject = defaultProject;
  }

  public FeatureRow setDefaults(FeatureRow featureRow) {
    String featureSetId = stripVersion(featureRow.getFeatureSet());
    featureSetId = applyDefaultProject(featureSetId);
    return featureRow.toBuilder().setFeatureSet(featureSetId).build();
  }

  // For backward compatibility. Will be deprecated eventually.
  private String stripVersion(String featureSetId) {
    String[] split = featureSetId.split(":");
    return split[0];
  }

  private String applyDefaultProject(String featureSetId) {
    String[] split = featureSetId.split("/");
    if (split.length == 1) {
      return defaultProject + "/" + featureSetId;
    }
    return featureSetId;
  }

  public FailedElement validateFeatureRow(
      String jobId, FeatureRow featureRow, FeatureSetSpec featureSetSpec) {
    String error = null;

    List<Field> fields = new ArrayList<>();

    FeatureSet featureSet = new FeatureSet(featureSetSpec);

    for (FieldProto.Field field : featureRow.getFieldsList()) {
      feast.spark.ingestion.common.Field fieldSpec = featureSet.getField(field.getName());
      if (fieldSpec == null) {
        // skip
        continue;
      }
      // If value is set in the FeatureRow, make sure the value type matches
      // that defined in FeatureSetSpec
      if (!field.getValue().getValCase().equals(ValueProto.Value.ValCase.VAL_NOT_SET)) {
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

    if (error == null) {
      return null;
    }

    FailedElement.Builder failedElement =
        FailedElement.newBuilder()
            .setTransformName("ValidateFeatureRow")
            .setJobName(jobId)
            .setPayload(featureRow.toString())
            .setErrorMessage(error)
            .setProjectName(featureSetSpec.getProject())
            .setFeatureSetName(featureSetSpec.getName());

    return failedElement.build();
  }
}
