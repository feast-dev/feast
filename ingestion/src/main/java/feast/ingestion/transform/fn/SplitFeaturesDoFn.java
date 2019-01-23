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

import feast.ingestion.transform.SplitFeatures.SplitStrategy;
import feast.ingestion.model.Specs;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.types.FeatureProto.Feature;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import feast.types.FeatureRowProto.FeatureRow;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import lombok.AllArgsConstructor;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

@AllArgsConstructor
public class SplitFeaturesDoFn<T> extends DoFn<FeatureRowExtended, FeatureRowExtended> {

  private SplitStrategy<T> splitStrategy;
  private Specs specs;

  @ProcessElement
  public void processElement(ProcessContext context) {
    FeatureRowExtended rowExtended = context.element();
    FeatureRow row = rowExtended.getRow();
    Map<TupleTag<FeatureRowExtended>, FeatureRow.Builder> taggedOutput = new HashMap<>();

    for (Feature feature : row.getFeaturesList()) {
      FeatureSpec featureSpec = specs.tryGetFeatureSpec(feature.getId());
      if (featureSpec == null) {
        continue;
      }

      TupleTag<FeatureRowExtended> tag = splitStrategy.getTag(featureSpec);
      FeatureRow.Builder builder = taggedOutput.get(tag);

      if (builder == null) {
        builder =
            FeatureRow.newBuilder()
                .setGranularity(row.getGranularity())
                .setEventTimestamp(row.getEventTimestamp())
                .setEntityName(row.getEntityName())
                .setEntityKey(row.getEntityKey());
      }
      builder.addFeatures(feature);
      taggedOutput.put(tag, builder);
    }

    for (Entry<TupleTag<FeatureRowExtended>, FeatureRow.Builder> entry : taggedOutput.entrySet()) {
      FeatureRowExtended featureRowExtended =
          FeatureRowExtended.newBuilder().mergeFrom(rowExtended).setRow(entry.getValue()).build();
      if (splitStrategy.isOutputTag(entry.getKey())) {
        context.output(entry.getKey(), featureRowExtended);
      } else {
        context.output(featureRowExtended);
      }
    }
  }
}
