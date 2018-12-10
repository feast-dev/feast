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

package feast.ingestion.transform;

import feast.ingestion.util.DateUtil;
import feast.types.FeatureRowExtendedProto.Attempt;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import feast.types.FeatureRowProto.FeatureRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTime;

public class ToFeatureRowExtended extends
    PTransform<PCollection<FeatureRow>, PCollection<FeatureRowExtended>> {

  @Override
  public PCollection<FeatureRowExtended> expand(PCollection<FeatureRow> input) {
    return input.apply(ParDo.of(new DoFn<FeatureRow, FeatureRowExtended>() {
      @ProcessElement
      public void processElement(@Element FeatureRow row, OutputReceiver<FeatureRowExtended> out) {
        out.output(FeatureRowExtended.newBuilder()
            .setRow(row)
            .setFirstSeen(DateUtil.toTimestamp(DateTime.now()))
            .setLastAttempt(Attempt.newBuilder()
                .setAttempts(0)
                .build())
            .build());
      }
    }));
  }
}
