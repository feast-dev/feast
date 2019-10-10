package feast.ingestion.transform;

import com.google.api.services.bigquery.model.TimePartitioning;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.BigQueryConfig;
import feast.core.StoreProto.Store.RedisConfig;
import feast.ingestion.options.ImportOptions;
import feast.store.serving.bigquery.FeatureRowToTableRowDoFn;
import feast.store.serving.redis.FeatureRowToRedisMutationDoFn;
import feast.store.serving.redis.RedisCustomIO;
import feast.types.FeatureRowProto.FeatureRow;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

@Slf4j
public class WriteFeaturesTransform extends PTransform<PCollection<FeatureRow>, PDone> {

  private final Store store;
  private final FeatureSetSpec featureSetSpec;

  public WriteFeaturesTransform(Store store, FeatureSetSpec featureSetSpec) {
    this.store = store;
    this.featureSetSpec = featureSetSpec;
  }

  @Override
  public PDone expand(PCollection<FeatureRow> input) {
    ImportOptions options =
        input.getPipeline().getOptions().as(ImportOptions.class);

    switch (store.getType()) {
      case REDIS:
        RedisConfig redisConfig = store.getRedisConfig();
        String redisHost = redisConfig.getHost();
        int redisPort = redisConfig.getPort();
        input
            .apply(
                "Create RedisMutation from FeatureRow",
                ParDo.of(new FeatureRowToRedisMutationDoFn(featureSetSpec)))
            .apply(RedisCustomIO.write(redisHost, redisPort));
        break;

      case BIGQUERY:
        BigQueryConfig bqConfig = store.getBigqueryConfig();
        String tableSpec =
            String.format(
                "%s:%s.%s_v%s",
                bqConfig.getProjectId(),
                bqConfig.getDatasetId(),
                featureSetSpec.getName(),
                featureSetSpec.getVersion());
        TimePartitioning timePartitioning =
            new TimePartitioning()
                .setType("DAY")
                .setField(FeatureRowToTableRowDoFn.getEventTimestampColumn());
        input
            .apply(
                "Create BigQuery TableRow from FeatureRow",
                ParDo.of(new FeatureRowToTableRowDoFn(options.getJobName())))
            .apply(
                BigQueryIO.writeTableRows()
                    .to(tableSpec)
                    .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                    .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                    .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                    .withTimePartitioning(timePartitioning));
        break;

      default:
        log.warn(
            "Store of type '{}' is not supported, no FeatureRows will be written.",
            store.getType());
        break;
    }
    return PDone.in(input.getPipeline());
  }
}
