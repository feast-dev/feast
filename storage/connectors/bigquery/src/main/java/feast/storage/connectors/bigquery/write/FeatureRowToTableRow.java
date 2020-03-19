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

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.util.Timestamps;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto.Field;
import java.util.Base64;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.joda.time.Instant;

// TODO: Validate FeatureRow against FeatureSetSpec
//       i.e. that the value types in FeatureRow matches against those in FeatureSetSpec

public class FeatureRowToTableRow implements SerializableFunction<FeatureRow, TableRow> {
  private static final String EVENT_TIMESTAMP_COLUMN = "event_timestamp";
  private static final String CREATED_TIMESTAMP_COLUMN = "created_timestamp";
  private static final String JOB_ID_COLUMN = "job_id";
  private final String jobId;

  public FeatureRowToTableRow(String jobId) {
    this.jobId = jobId;
  }

  public static String getEventTimestampColumn() {
    return EVENT_TIMESTAMP_COLUMN;
  }

  public TableRow apply(FeatureRow featureRow) {

    TableRow tableRow = new TableRow();
    tableRow.set(EVENT_TIMESTAMP_COLUMN, Timestamps.toString(featureRow.getEventTimestamp()));
    tableRow.set(CREATED_TIMESTAMP_COLUMN, Instant.now().toString());
    tableRow.set(JOB_ID_COLUMN, jobId);

    for (Field field : featureRow.getFieldsList()) {
      switch (field.getValue().getValCase()) {
        case BYTES_VAL:
          tableRow.set(
              field.getName(),
              Base64.getEncoder().encodeToString(field.getValue().getBytesVal().toByteArray()));
          break;
        case STRING_VAL:
          tableRow.set(field.getName(), field.getValue().getStringVal());
          break;
        case INT32_VAL:
          tableRow.set(field.getName(), field.getValue().getInt32Val());
          break;
        case INT64_VAL:
          tableRow.set(field.getName(), field.getValue().getInt64Val());
          break;
        case DOUBLE_VAL:
          tableRow.set(field.getName(), field.getValue().getDoubleVal());
          break;
        case FLOAT_VAL:
          tableRow.set(field.getName(), field.getValue().getFloatVal());
          break;
        case BOOL_VAL:
          tableRow.set(field.getName(), field.getValue().getBoolVal());
          break;
        case BYTES_LIST_VAL:
          tableRow.set(
              field.getName(),
              field.getValue().getBytesListVal().getValList().stream()
                  .map(x -> Base64.getEncoder().encodeToString(x.toByteArray()))
                  .collect(Collectors.toList()));
          break;
        case STRING_LIST_VAL:
          tableRow.set(field.getName(), field.getValue().getStringListVal().getValList());
          break;
        case INT32_LIST_VAL:
          tableRow.set(field.getName(), field.getValue().getInt32ListVal().getValList());
          break;
        case INT64_LIST_VAL:
          tableRow.set(field.getName(), field.getValue().getInt64ListVal().getValList());
          break;
        case DOUBLE_LIST_VAL:
          tableRow.set(field.getName(), field.getValue().getDoubleListVal().getValList());
          break;
        case FLOAT_LIST_VAL:
          tableRow.set(field.getName(), field.getValue().getFloatListVal().getValList());
          break;
        case BOOL_LIST_VAL:
          tableRow.set(field.getName(), field.getValue().getBytesListVal().getValList());
          break;
        case VAL_NOT_SET:
          break;
      }
    }

    return tableRow;
  }
}
