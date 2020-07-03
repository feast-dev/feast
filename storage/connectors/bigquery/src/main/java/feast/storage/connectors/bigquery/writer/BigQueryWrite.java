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
package feast.storage.connectors.bigquery.writer;

import com.google.api.services.bigquery.model.*;
import com.google.cloud.bigquery.DatasetId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.storage.api.writer.FailedElement;
import feast.storage.api.writer.WriteResult;
import feast.storage.connectors.bigquery.compression.CompactFeatureRows;
import feast.storage.connectors.bigquery.compression.FeatureRowsBatch;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.slf4j.Logger;

/**
 * A {@link PTransform} that writes {@link FeatureRow FeatureRows} to the specified BigQuery
 * dataset, and returns a {@link WriteResult} containing the unsuccessful writes. Since Bigquery
 * does not output failed writes, we cannot emit those.
 */
public class BigQueryWrite extends PTransform<PCollection<FeatureRow>, WriteResult> {
  private static final Logger log = org.slf4j.LoggerFactory.getLogger(BigQueryWrite.class);

  private static final Duration BIGQUERY_DEFAULT_WRITE_TRIGGERING_FREQUENCY =
      Duration.standardMinutes(1);

  private static final Duration BIGQUERY_JOB_MAX_EXPECTING_RESULT_TIME = Duration.standardHours(1);
  private static final int BIGQUERY_MAX_JOB_RETRIES = 20;
  private static final int DEFAULT_COMPACTION_BATCH_SIZE = 10000;

  private DatasetId destination;
  private PCollectionView<Map<String, Iterable<TableSchema>>> schemas;

  private Duration triggeringFrequency = BIGQUERY_DEFAULT_WRITE_TRIGGERING_FREQUENCY;
  private Duration expectingResultTime = BIGQUERY_JOB_MAX_EXPECTING_RESULT_TIME;
  private BigQueryServices testServices;
  private int compactionBatchSize = DEFAULT_COMPACTION_BATCH_SIZE;

  public BigQueryWrite(
      DatasetId destination, PCollectionView<Map<String, Iterable<TableSchema>>> schemas) {
    this.destination = destination;
    this.schemas = schemas;
  }

  public BigQueryWrite withTriggeringFrequency(Duration triggeringFrequency) {
    this.triggeringFrequency = triggeringFrequency;
    return this;
  }

  public BigQueryWrite withTestServices(BigQueryServices testServices) {
    this.testServices = testServices;
    return this;
  }

  public BigQueryWrite withExpectingResultTime(Duration expectingResultTime) {
    this.expectingResultTime = expectingResultTime;
    return this;
  }

  public BigQueryWrite withCompactionBatchSize(int batchSize) {
    this.compactionBatchSize = batchSize;
    return this;
  }

  /**
   * BigQuery writer 1. choose destination based on featureSetName {@link
   * FeatureDynamicDestinations} 2. dynamically pull destination's schema from schemas' view 3.
   * convert {@link FeatureRow} into {@link TableRow} 4. group input into fixed windows (configured
   * with setTriggeringFrequency) 5. write to bq (via BATCH FILE LOADING) {@link
   * BatchLoadsWithResult} 6. join bq job output with input to produce successful inserts
   *
   * @param input stream of FeatureRows to write
   * @return stream of successfully inserted FeatureRows
   */
  @Override
  public WriteResult expand(PCollection<FeatureRow> input) {
    String jobName = input.getPipeline().getOptions().getJobName();

    FeatureDynamicDestinations dynamicDestinations = new FeatureDynamicDestinations();

    BatchLoadsWithResult.Builder<String> writerBuilder =
        BatchLoadsWithResult.<String>create()
            .setWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .setCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .setDynamicDestinations(dynamicDestinations)
            .setDestinationCoder(StringUtf8Coder.of())
            .setElementCoder(TableRowJsonCoder.of())
            .setTriggeringFrequency(triggeringFrequency)
            .setMaxRetryJobs(BIGQUERY_MAX_JOB_RETRIES)
            .setSchemaUpdateOptions(
                ImmutableSet.of(BigQueryIO.Write.SchemaUpdateOption.ALLOW_FIELD_ADDITION));

    if (testServices != null) {
      writerBuilder.setBigQueryServices(testServices);
    }

    PCollection<FeatureRow> inputInFixedWindow =
        input.apply(
            "BatchingInput",
            Window.<FeatureRow>into(FixedWindows.of(triggeringFrequency))
                .withAllowedLateness(Duration.ZERO)
                .discardingFiredPanes());

    PCollection<KV<TableDestination, String>> insertionResult =
        inputInFixedWindow
            .apply(
                "PrepareWrite",
                new PrepareWrite<>(dynamicDestinations, new FeatureRowToTableRow(jobName)))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), TableRowJsonCoder.of()))
            .apply("WriteTableRowToBigQuery", writerBuilder.build());

    PCollection<FeatureRow> successfulInserts =
        mergeInputWithResult(inputInFixedWindow, insertionResult);

    // Since BigQueryIO does not support emitting failure writes, we set failedElements to
    // an empty stream
    PCollection<FailedElement> failedElements =
        input
            .getPipeline()
            .apply(Create.of(""))
            .apply(
                "dummy",
                ParDo.of(
                    new DoFn<String, FailedElement>() {
                      @ProcessElement
                      public void processElement(ProcessContext context) {}
                    }));

    return WriteResult.in(input.getPipeline(), successfulInserts, failedElements);
  }

  /**
   * Join input stream of FeatureRows with output stream from write jobs to produce rows that was
   * successfully inserted.
   *
   * <p>In order to join we expect both streams to have identical windowing strategy. Join key - is
   * write destination (table reference), hence as soon as bq load job responsible for specific
   * table succeeded - we consider all rows routed to this destination in this window(!) as
   * successfully written
   *
   * @param inputInFixedWindow input stream in fixed window
   * @param successful output stream from BatchLoader {@link BatchLoadsWithResult}
   * @return collection of successfully inserted rows
   */
  private PCollection<FeatureRow> mergeInputWithResult(
      PCollection<FeatureRow> inputInFixedWindow,
      PCollection<KV<TableDestination, String>> successful) {
    final TupleTag<FeatureRowsBatch> inputTag = new TupleTag<>();
    final TupleTag<String> successTag = new TupleTag<>();

    PCollection<KV<String, FeatureRowsBatch>> insertedRows =
        inputInFixedWindow
            .apply(
                "MakeElementKey",
                ParDo.of(
                    new DoFn<FeatureRow, KV<String, FeatureRow>>() {
                      @ProcessElement
                      public void process(ProcessContext c) {
                        FeatureRow element = c.element();
                        c.output(
                            KV.of(
                                BigQuerySinkHelpers.getTableDestination(
                                        destination, element.getFeatureSet())
                                    .getTableSpec(),
                                element));
                      }
                    }))
            .apply(new CompactFeatureRows(compactionBatchSize));

    PCollection<KV<String, CoGbkResult>> inputWithResult =
        KeyedPCollectionTuple.of(inputTag, insertedRows)
            .and(
                successTag,
                successful.apply(
                    "MakeResultKey",
                    ParDo.of(
                        new DoFn<KV<TableDestination, String>, KV<String, String>>() {
                          @ProcessElement
                          public void process(ProcessContext c) {
                            c.output(
                                KV.of(c.element().getKey().getTableSpec(), c.element().getValue()));
                          }
                        })))
            .apply(CoGroupByKey.create());

    return inputWithResult.apply(
        "ProduceSuccessfulInserts",
        ParDo.of(
            new DoFn<KV<String, CoGbkResult>, FeatureRow>() {
              @ProcessElement
              public void process(ProcessContext c) {
                CoGbkResult result = c.element().getValue();
                boolean ready = result.getAll(successTag).iterator().hasNext();
                if (!ready) {
                  return;
                }

                result
                    .getAll(inputTag)
                    .forEach(rows -> rows.getFeatureRows().forEachRemaining(c::output));
              }
            }));
  }

  /**
   * DynamicDestination router allocates incoming {@link FeatureRow} to tables in BigQuery based on
   * FeatureSet reference extracted from this row.
   *
   * <p>getDestination is called for every item and simply returns grouping key {@link String}.
   *
   * <p>On window triggering getTable and getSchema are called once per each group.
   *
   * <p>getSchema provides latest table schema calculated from PCollection&lt;FeatureSetSpec&gt;.
   * This schema is attached to each BQ Load job to update tables in-flight.
   */
  private class FeatureDynamicDestinations extends DynamicDestinations<FeatureRow, String> {
    @Override
    public String getDestination(ValueInSingleWindow<FeatureRow> element) {
      return element.getValue().getFeatureSet();
    }

    @Override
    public List<PCollectionView<?>> getSideInputs() {
      return ImmutableList.of(schemas);
    }

    @Override
    public TableDestination getTable(String featureSetKey) {
      return BigQuerySinkHelpers.getTableDestination(destination, featureSetKey);
    }

    @Override
    public TableSchema getSchema(String featureSet) {
      Map<String, Iterable<TableSchema>> schemasValue = sideInput(schemas);
      Iterable<TableSchema> schemasIt = schemasValue.get(featureSet);
      if (schemasIt == null) {
        return null;
      }
      return Iterators.getLast(schemasIt.iterator());
    }
  }
}
