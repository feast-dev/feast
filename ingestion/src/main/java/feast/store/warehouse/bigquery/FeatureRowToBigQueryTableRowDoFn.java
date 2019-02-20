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

package feast.store.warehouse.bigquery;

import com.google.api.services.bigquery.model.TableRow;
import feast.ingestion.util.DateUtil;
import feast.ingestion.model.Specs;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.types.FeatureProto.Feature;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.ValueProto.Value;
import lombok.AllArgsConstructor;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

@AllArgsConstructor
public class FeatureRowToBigQueryTableRowDoFn extends DoFn<FeatureRowExtended, TableRow> {

  private static final String ENTITY_KEY_COLUMN = "id";
  private static final String EVENT_TIMESTAMP_COLUMN = "event_timestamp";
  private static final String CREATED_TIMESTAMP_COLUMN = "created_timestamp";

  private Specs specs;

  @ProcessElement
  public void processElement(ProcessContext context) {
    context.output(toTableRow(context.element()));
  }

  public TableRow toTableRow(FeatureRowExtended featureRowExtended) {
    FeatureRow featureRow = featureRowExtended.getRow();
    TableRow tableRow = new TableRow();

    String entityKey = featureRow.getEntityKey();
    tableRow.set(ENTITY_KEY_COLUMN, entityKey);
    tableRow.set(
        EVENT_TIMESTAMP_COLUMN,
        ValueBigQueryBuilder.bigQueryObjectOf(
            Value.newBuilder().setTimestampVal(featureRow.getEventTimestamp())));
    tableRow.set(
        CREATED_TIMESTAMP_COLUMN,
        ValueBigQueryBuilder.bigQueryObjectOf(
            Value.newBuilder()
                .setTimestampVal(DateUtil.toTimestamp(DateTime.now(DateTimeZone.UTC)))));

    for (Feature feature : featureRow.getFeaturesList()) {
      Object featureValue = ValueBigQueryBuilder.bigQueryObjectOf(feature.getValue());
      FeatureSpec featureSpec = specs.getFeatureSpec(feature.getId());
      tableRow.set(featureSpec.getName(), featureValue);
    }
    return tableRow;
  }
}
