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
package feast.ingestion.transform;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import feast.storage.api.writer.FailedElement;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

@AutoValue
public abstract class WriteFailedElementToBigQuery
    extends PTransform<PCollection<FailedElement>, WriteResult> {
  public abstract String getTableSpec();

  public abstract String getJsonSchema();

  public static Builder newBuilder() {
    return new AutoValue_WriteFailedElementToBigQuery.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    /**
     * @param tableSpec Table spec should follow the format "PROJECT_ID:DATASET_ID.TABLE_ID". Table
     *     will be created if not exists.
     */
    public abstract Builder setTableSpec(String tableSpec);

    /**
     * @param jsonSchema JSON string describing the <a
     *     href="https://cloud.google.com/bigquery/docs/schemas#specifying_a_json_schema_file">schema</a>
     *     of the table.
     */
    public abstract Builder setJsonSchema(String jsonSchema);

    public abstract WriteFailedElementToBigQuery build();
  }

  @Override
  public WriteResult expand(PCollection<FailedElement> failedElements) {
    return failedElements
        .apply("FailedElementToTableRow", ParDo.of(new FailedElementToTableRowFn()))
        .apply(
            "WriteFailedElementsToBigQuery",
            BigQueryIO.writeTableRows()
                .to(getTableSpec())
                .withJsonSchema(getJsonSchema())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND));
  }

  public static class FailedElementToTableRowFn extends DoFn<FailedElement, TableRow> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      final FailedElement element = context.element();
      final TableRow tableRow =
          new TableRow()
              .set("timestamp", element.getTimestamp().toString())
              .set("job_name", element.getJobName())
              .set("transform_name", element.getTransformName())
              .set("payload", element.getPayload())
              .set("error_message", element.getErrorMessage())
              .set("stack_trace", element.getStackTrace());
      context.output(tableRow);
    }
  }
}
