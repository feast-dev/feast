package feast.ingestion.transform;

import com.google.api.services.bigquery.model.TableDataInsertAllResponse.InsertErrors;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.auto.value.AutoValue;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.BigQueryConfig;
import feast.core.StoreProto.Store.RedisConfig;
import feast.core.StoreProto.Store.StoreType;
import feast.ingestion.options.ImportOptions;
import feast.ingestion.utils.ResourceUtil;
import feast.ingestion.values.FailedElement;
import feast.store.serving.bigquery.FeatureRowToTableRow;
import feast.store.serving.bigquery.GetTableReference;
import feast.store.serving.redis.FeatureRowToRedisMutationDoFn;
import feast.store.serving.redis.RedisCustomIO;
import feast.types.FeatureRowProto.FeatureRow;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.Logger;

@AutoValue
public abstract class WriteToStore extends PTransform<PCollection<FeatureRow>, PDone> {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(WriteToStore.class);

  public abstract Store getStore();

  public abstract Map<String, FeatureSetSpec> getFeatureSetSpecs();

  public static Builder newBuilder() {
    return new AutoValue_WriteToStore.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setStore(Store store);

    public abstract Builder setFeatureSetSpecs(Map<String, FeatureSetSpec> featureSetSpecs);

    public abstract WriteToStore build();
  }

  @Override
  public PDone expand(PCollection<FeatureRow> input) {
    ImportOptions options = input.getPipeline().getOptions().as(ImportOptions.class);
    StoreType storeType = getStore().getType();

    switch (storeType) {
      case REDIS:
        RedisConfig redisConfig = getStore().getRedisConfig();
        input
            .apply(
                "FeatureRowToRedisMutation",
                ParDo.of(new FeatureRowToRedisMutationDoFn(getFeatureSetSpecs())))
            .apply(
                "WriteRedisMutationToRedis",
                RedisCustomIO.write(redisConfig.getHost(), redisConfig.getPort()));
        break;
      case BIGQUERY:

        BigQueryConfig bigqueryConfig = getStore().getBigqueryConfig();
        TimePartitioning timePartitioning =
            new TimePartitioning()
                .setType("DAY")
                .setField(FeatureRowToTableRow.getEventTimestampColumn());

        WriteResult bigqueryWriteResult =
            input
                .apply(
                    "WriteTableRowToBigQuery",
                    BigQueryIO.<FeatureRow>write()
                        .to(new GetTableReference(bigqueryConfig.getProjectId(),
                            bigqueryConfig.getDatasetId()))
                        .withFormatFunction(new FeatureRowToTableRow(options.getJobName()))
                        .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                        .withExtendedErrorInfo()
                        .withMethod(Method.STREAMING_INSERTS)
                        .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                        .withTimePartitioning(timePartitioning));

        if (options.getDeadLetterTableSpec() != null) {
          bigqueryWriteResult
              .getFailedInsertsWithErr()
              .apply(
                  "WrapBigQueryInsertionError",
                  ParDo.of(
                      new DoFn<BigQueryInsertError, FailedElement>() {
                        @ProcessElement
                        public void processElement(ProcessContext context) {
                          InsertErrors error = context.element().getError();
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
                      }))
              .apply(
                  WriteFailedElementToBigQuery.newBuilder()
                      .setTableSpec(options.getDeadLetterTableSpec())
                      .setJsonSchema(ResourceUtil.getDeadletterTableSchemaJson())
                      .build());
        }
        break;
      default:
        log.error("Store type '{}' is not supported. No Feature Row will be written.", storeType);
        break;
    }

    return PDone.in(input.getPipeline());
  }
}
