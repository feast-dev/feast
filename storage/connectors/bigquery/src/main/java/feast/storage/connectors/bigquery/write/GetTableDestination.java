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
package feast.storage.connectors.bigquery.write;

import com.google.api.services.bigquery.model.TimePartitioning;
import feast.types.FeatureRowProto.FeatureRow;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.ValueInSingleWindow;

public class GetTableDestination
    implements SerializableFunction<ValueInSingleWindow<FeatureRow>, TableDestination> {

  private String projectId;
  private String datasetId;

  public GetTableDestination(String projectId, String datasetId) {
    this.projectId = projectId;
    this.datasetId = datasetId;
  }

  @Override
  public TableDestination apply(ValueInSingleWindow<FeatureRow> input) {
    String[] split = input.getValue().getFeatureSet().split(":");
    String[] splitName = split[0].split("/");

    TimePartitioning timePartitioning =
        new TimePartitioning()
            .setType("DAY")
            .setField(FeatureRowToTableRow.getEventTimestampColumn());

    return new TableDestination(
        String.format(
            "%s:%s.%s_%s_v%s", projectId, datasetId, splitName[0], splitName[1], split[1]),
        String.format("Feast table for %s", input.getValue().getFeatureSet()),
        timePartitioning);
  }
}
