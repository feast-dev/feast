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

package feast.ingestion.transform.fn;

import feast.ingestion.model.Specs;
import feast.ingestion.model.Values;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.types.FeatureProto.Feature;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import feast.types.FeatureRowProto.FeatureRow;
import lombok.AllArgsConstructor;

/**
 * Convert value's type of feature inside {@link FeatureRowExtended} into the value type as
 * specified in corresponding feature spec.
 */
@AllArgsConstructor
public class ConvertTypesDoFn extends BaseFeatureDoFn {
  private Specs specs;

  @Override
  public void processElementImpl(ProcessContext context) {
    FeatureRowExtended rowExtended = context.element();
    FeatureRow row = rowExtended.getRow();
    FeatureRow.Builder rowBuilder = FeatureRow.newBuilder();
    rowBuilder
        .setEntityName(row.getEntityName())
        .setEventTimestamp(row.getEventTimestamp())
        .setEntityKey(row.getEntityKey());

    for (Feature feature : row.getFeaturesList()) {
      String featureId = feature.getId();
      FeatureSpec featureSpec = specs.tryGetFeatureSpec(featureId);
      if (featureSpec == null) {
        continue;
      }

      rowBuilder.setGranularity(featureSpec.getGranularity());
      rowBuilder.addFeatures(
          Feature.newBuilder()
              .setId(featureId)
              .setValue(Values.asType(feature.getValue(), featureSpec.getValueType())));
    }
    context.output(
        FeatureRowExtended.newBuilder()
            .setRow(rowBuilder)
            .setLastAttempt(rowExtended.getLastAttempt())
            .build());
  }
}
