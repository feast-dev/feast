package org.apache.beam.sdk.io.gcp.bigquery;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.resolveTempLocation;
import static org.apache.beam.vendor.grpc.v1p21p0.com.google.common.base.Preconditions.checkArgument;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class BatchLoadsWithResult<DestinationT>
    extends PTransform<
    PCollection<KV<DestinationT, TableRow>>, PCollection<KV<TableDestination, String>>> {
  static final Logger LOG = LoggerFactory.getLogger(BatchLoadsWithResult.class);

  @VisibleForTesting
  // Maximum number of files in a single partition.
  static final int DEFAULT_MAX_FILES_PER_PARTITION = 10000;

  @VisibleForTesting
  // Maximum number of bytes in a single partition -- 11 TiB just under BQ's 12 TiB limit.
  static final long DEFAULT_MAX_BYTES_PER_PARTITION = 11 * (1L << 40);

  // The maximum size of a single file - 4TiB, just under the 5 TiB limit.
  static final long DEFAULT_MAX_FILE_SIZE = 4 * (1L << 40);

  static final int DEFAULT_MAX_RETRY_JOBS = 3;

  static final int FILE_TRIGGERING_RECORD_COUNT = 500000;

  @Nullable
  abstract BigQueryServices getBigQueryServices();

  abstract boolean getIgnoreUnknownValues();

  abstract BigQueryIO.Write.WriteDisposition getWriteDisposition();

  abstract BigQueryIO.Write.CreateDisposition getCreateDisposition();

  abstract Set<BigQueryIO.Write.SchemaUpdateOption> getSchemaUpdateOptions();

  abstract DynamicDestinations<?, DestinationT> getDynamicDestinations();

  abstract Coder<DestinationT> getDestinationCoder();

  abstract Duration getTriggeringFrequency();

  @Nullable
  abstract ValueProvider<String> getCustomGcsTempLocation();

  @Nullable
  abstract ValueProvider<String> getLoadJobProjectId();

  abstract Coder<TableRow> getElementCoder();

  abstract RowWriterFactory<TableRow, DestinationT> getRowWriterFactory();

  @Nullable
  abstract String getKmsKey();

  abstract int getMaxRetryJobs();

  @AutoValue.Builder
  public abstract static class Builder<DestinationT> {
    public abstract Builder<DestinationT> setBigQueryServices(BigQueryServices bigQueryServices);

    public abstract Builder<DestinationT> setIgnoreUnknownValues(boolean ignoreUnknownValues);

    public abstract Builder<DestinationT> setWriteDisposition(
        BigQueryIO.Write.WriteDisposition writeDisposition);

    public abstract Builder<DestinationT> setCreateDisposition(
        BigQueryIO.Write.CreateDisposition createDisposition);

    public abstract Builder<DestinationT> setSchemaUpdateOptions(
        Set<BigQueryIO.Write.SchemaUpdateOption> schemaUpdateOptions);

    public abstract Builder<DestinationT> setDynamicDestinations(
        DynamicDestinations<?, DestinationT> dynamicDestinations);

    public abstract Builder<DestinationT> setDestinationCoder(Coder<DestinationT> destinationCoder);

    public abstract Builder<DestinationT> setTriggeringFrequency(Duration triggeringFrequency);

    public abstract Builder<DestinationT> setCustomGcsTempLocation(
        @Nullable ValueProvider<String> customGcsTempLocation);

    public abstract Builder<DestinationT> setLoadJobProjectId(
        @Nullable ValueProvider<String> loadJobProjectId);

    public abstract Builder<DestinationT> setElementCoder(Coder<TableRow> elementCoder);

    public abstract Builder<DestinationT> setRowWriterFactory(
        RowWriterFactory<TableRow, DestinationT> rowWriterFactory);

    public abstract Builder<DestinationT> setKmsKey(@Nullable String kmsKey);

    public abstract Builder<DestinationT> setMaxRetryJobs(int maxRetryJobs);

    public abstract BatchLoadsWithResult<DestinationT> build();
  }

  public static <DestinationT> Builder<DestinationT> create() {
    return new AutoValue_BatchLoadsWithResult.Builder<DestinationT>()
        .setIgnoreUnknownValues(false)
        .setBigQueryServices(new BigQueryServicesImpl())
        .setRowWriterFactory(RowWriterFactory.tableRows(SerializableFunctions.identity()))
        .setSchemaUpdateOptions(Collections.emptySet())
        .setMaxRetryJobs(DEFAULT_MAX_RETRY_JOBS);
  }

  public PCollection<KV<TableDestination, String>> expand(
      PCollection<KV<DestinationT, TableRow>> input) {

    // we assume that input must be already windowed and we allow only fixed window
    // so our internal generator (JobIdPrefix) would be compatible (hence, joinable)
    checkArgument(
        input.getWindowingStrategy().getWindowFn() instanceof FixedWindows,
        "Input to BQ writer must be windowed in advance");

    final PCollection<String> loadJobIdPrefixView = createLoadJobIdPrefixView(input);
    final PCollectionView<String> tempFilePrefixView =
        createTempFilePrefixView(loadJobIdPrefixView);

    PCollection<WriteBundlesToFiles.Result<DestinationT>> results =
        input
            .apply("WindowWithTrigger",
                Window.<KV<DestinationT, TableRow>>configure()
                    .triggering(
                        Repeatedly.forever(
                            AfterPane.elementCountAtLeast(FILE_TRIGGERING_RECORD_COUNT)))
                    .discardingFiredPanes())
            .apply(
                "PutAllRowsInSingleShard",
                ParDo.of(
                    new DoFn<KV<DestinationT, TableRow>, KV<ShardedKey<DestinationT>, TableRow>>() {
                      @ProcessElement
                      public void process(ProcessContext c) {
                        c.output(
                            KV.of(ShardedKey.of(c.element().getKey(), 0), c.element().getValue()));
                      }
                    }))
            .setCoder(KvCoder.of(ShardedKeyCoder.of(getDestinationCoder()), getElementCoder()))
            .apply("GroupByDestination", GroupByKey.create())
            .apply(
                "WriteGroupedRecords",
                ParDo.of(
                    new WriteGroupedRecordsToFiles<>(
                        tempFilePrefixView, DEFAULT_MAX_FILE_SIZE, getRowWriterFactory()))
                    .withSideInputs(tempFilePrefixView))
            .setCoder(WriteBundlesToFiles.ResultCoder.of(getDestinationCoder()));

    TupleTag<KV<ShardedKey<DestinationT>, List<String>>> multiPartitionsTag =
        new TupleTag<>("multiPartitionsTag");
    TupleTag<KV<ShardedKey<DestinationT>, List<String>>> singlePartitionTag =
        new TupleTag<>("singlePartitionTag");

    // Copied from original BatchLoads
    PCollectionTuple partitions =
        results
            .apply(
                Window.<WriteBundlesToFiles.Result<DestinationT>>configure()
                    .triggering(DefaultTrigger.of())
            )
            .apply("AttachSingletonKey", WithKeys.of((Void) null))
            .setCoder(
                KvCoder.of(
                    VoidCoder.of(), WriteBundlesToFiles.ResultCoder.of(getDestinationCoder())))
            .apply("GroupOntoSingleton", GroupByKey.create())
            .apply("ExtractResultValues", Values.create())
            .apply(
                "WritePartitionTriggered",
                ParDo.of(
                    new WritePartition<>(
                        false,
                        getDynamicDestinations(),
                        tempFilePrefixView,
                        DEFAULT_MAX_FILES_PER_PARTITION,
                        DEFAULT_MAX_BYTES_PER_PARTITION,
                        multiPartitionsTag,
                        singlePartitionTag,
                        getRowWriterFactory()))
                    .withSideInputs(tempFilePrefixView)
                    .withOutputTags(multiPartitionsTag, TupleTagList.of(singlePartitionTag)));

    partitions
        .get(multiPartitionsTag)
        .setCoder(
            KvCoder.of(
                ShardedKeyCoder.of(NullableCoder.of(getDestinationCoder())),
                ListCoder.of(StringUtf8Coder.of())));

    return writeSinglePartitionWithResult(
        partitions.get(singlePartitionTag), loadJobIdPrefixView.apply(View.asSingleton()));
  }

  private PCollection<String> createLoadJobIdPrefixView(
      PCollection<KV<DestinationT, TableRow>> input) {
    // We generate new JobId per each (input) window
    // To keep BQ job's name unique
    // Windowing of this generator is expected to be synchronized with input window
    // So generated ids can be applied as side input
    return input
        .apply(
            "EraseKey",
            ParDo.of(
                new DoFn<KV<DestinationT, TableRow>, KV<Void, TableRow>>() {
                  @ProcessElement
                  public void process(ProcessContext c) {
                    c.output(KV.of(null, c.element().getValue()));
                  }
                }))
        .apply(
            "CreateJobId",
            ParDo.of(
                new DoFn<KV<Void, TableRow>, String>() {
                  @StateId("generatedForWindow")
                  private final StateSpec<ValueState<Boolean>> generatedForWindow = StateSpecs.value(BooleanCoder.of());

                  @ProcessElement
                  public void process(
                      ProcessContext c,
                      BoundedWindow w,
                      @StateId("generatedForWindow") ValueState<Boolean> generatedForWindow) {

                    if (generatedForWindow.read() != null) {
                      return;
                    }

                    generatedForWindow.write(true);

                    c.output(
                        String.format(
                            "beam_load_%s_%s",
                            c.getPipelineOptions().getJobName().replaceAll("-", ""),
                            BigQueryHelpers.randomUUIDString()));

                    LOG.info("Pane {}, start: {}, last: {}", c.pane().getIndex(), c.pane().isFirst(), c.pane().isLast());
                    LOG.info("[BQ] New window {}, {}", c.timestamp(), w.maxTimestamp());
                  }
                }));
  }

  private PCollectionView<String> createTempFilePrefixView(final PCollection<String> jobId) {
    return jobId
        .apply(
            "GetTempFilePrefix",
            ParDo.of(
                new DoFn<String, String>() {
                  @ProcessElement
                  public void getTempFilePrefix(ProcessContext c, BoundedWindow w) {
                    String tempLocationRoot;
                    if (getCustomGcsTempLocation() != null) {
                      tempLocationRoot = getCustomGcsTempLocation().get();
                    } else {
                      tempLocationRoot = c.getPipelineOptions().getTempLocation();
                    }
                    String tempLocation =
                        resolveTempLocation(tempLocationRoot, "BigQueryWriteTemp", c.element());
                    LOG.info("[BQ] temp location generated {}, {}", tempLocation, w.maxTimestamp());
                    c.output(tempLocation);
                  }
                }))
        .apply("TempFilePrefixView", View.asSingleton());
  }

  PCollection<KV<TableDestination, String>> writeSinglePartitionWithResult(
      PCollection<KV<ShardedKey<DestinationT>, List<String>>> input,
      PCollectionView<String> loadJobIdPrefixView) {
    List<PCollectionView<?>> sideInputs = Lists.newArrayList(loadJobIdPrefixView);
    sideInputs.addAll(getDynamicDestinations().getSideInputs());
    Coder<KV<ShardedKey<DestinationT>, List<String>>> partitionsCoder =
        KvCoder.of(
            ShardedKeyCoder.of(NullableCoder.of(getDestinationCoder())),
            ListCoder.of(StringUtf8Coder.of()));
    // Write single partition to final table
    return input
        .setCoder(partitionsCoder)
        // Reshuffle will distribute this among multiple workers, and also guard against
        // reexecution of the WritePartitions step once WriteTables has begun.
        .apply("SinglePartitionsReshuffle", Reshuffle.of())
        .apply(
            "SinglePartitionWriteTables",
            new WriteTables<>(
                false,
                getBigQueryServices(),
                loadJobIdPrefixView,
                getWriteDisposition(),
                getCreateDisposition(),
                sideInputs,
                getDynamicDestinations(),
                getLoadJobProjectId(),
                getMaxRetryJobs(),
                getIgnoreUnknownValues(),
                getKmsKey(),
                getRowWriterFactory().getSourceFormat(),
                getSchemaUpdateOptions()));
  }
}
