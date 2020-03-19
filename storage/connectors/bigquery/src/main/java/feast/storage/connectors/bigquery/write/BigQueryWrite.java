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
package feast.storage.connectors.bigquery.write;

import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.DatasetId;
import feast.storage.api.write.FailedElement;
import feast.storage.api.write.WriteResult;
import feast.types.FeatureRowProto;
import java.io.IOException;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;

/**
 * A {@link PTransform} that writes {@link FeatureRowProto FeatureRows} to the specified BigQuery
 * dataset, and returns a {@link WriteResult} containing the unsuccessful writes. Since Bigquery
 * does not output successful writes, we cannot emit those, and so no success metrics will be
 * captured if this sink is used.
 */
public class BigQueryWrite
    extends PTransform<PCollection<FeatureRowProto.FeatureRow>, WriteResult> {
  private static final Logger log = org.slf4j.LoggerFactory.getLogger(BigQueryWrite.class);

  // Destination dataset
  private DatasetId destination;

  public BigQueryWrite(DatasetId destination) {
    this.destination = destination;
  }

  @Override
  public WriteResult expand(PCollection<FeatureRowProto.FeatureRow> input) {
    String jobName = input.getPipeline().getOptions().getJobName();
    org.apache.beam.sdk.io.gcp.bigquery.WriteResult bigqueryWriteResult =
        input.apply(
            "WriteTableRowToBigQuery",
            BigQueryIO.<FeatureRowProto.FeatureRow>write()
                .to(new GetTableDestination(destination.getProject(), destination.getDataset()))
                .withFormatFunction(new FeatureRowToTableRow(jobName))
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withExtendedErrorInfo()
                .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()));

    PCollection<FailedElement> failedElements =
        bigqueryWriteResult
            .getFailedInsertsWithErr()
            .apply(
                "WrapBigQueryInsertionError",
                ParDo.of(
                    new DoFn<BigQueryInsertError, FailedElement>() {
                      @ProcessElement
                      public void processElement(ProcessContext context) {
                        TableDataInsertAllResponse.InsertErrors error =
                            context.element().getError();
                        TableRow row = context.element().getRow();
                        try {
                          context.output(
                              FailedElement.newBuilder()
                                  .setErrorMessage(error.toPrettyString())
                                  .setPayload(row.toPrettyString())
                                  .setJobName(context.getPipelineOptions().getJobName())
                                  .setTransformName("WriteTableRowToBigQuery")
                                  .build());
                        } catch (IOException e) {
                          log.error(e.getMessage());
                        }
                      }
                    }));

    // Since BigQueryIO does not support emitting successful writes, we set successfulInserts to
    // an empty stream,
    // and no metrics will be collected.
    PCollection<FeatureRowProto.FeatureRow> successfulInserts =
        input.apply(
            "dummy",
            ParDo.of(
                new DoFn<FeatureRowProto.FeatureRow, FeatureRowProto.FeatureRow>() {
                  @ProcessElement
                  public void processElement(ProcessContext context) {}
                }));

    return WriteResult.in(input.getPipeline(), successfulInserts, failedElements);
  }
}
