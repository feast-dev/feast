package feast.ingestion.transform;

import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.RedisConfig;
import feast.store.serving.redis.FeatureRowToRedisMutationDoFn;
import feast.store.serving.redis.RedisCustomIO;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

@Slf4j
@AllArgsConstructor
public class WriteFeaturesTransform extends PTransform<PCollection<FeatureRowExtended>, PDone> {

  private Store sinkStorageSpec;
  private FeatureSetSpec featureSetSpec;

  @Override
  public PDone expand(PCollection<FeatureRowExtended> input) {
    switch (sinkStorageSpec.getType()) {
      case REDIS:
        RedisConfig redisConfig = sinkStorageSpec.getRedisConfig();
        String redisHost = redisConfig.getHost();
        int redisPort = redisConfig.getPort();
        input
            .apply(
                "Create RedisMutation from FeatureRow",
                ParDo.of(new FeatureRowToRedisMutationDoFn(featureSetSpec)))
            .apply(RedisCustomIO.write(redisHost, redisPort));
        break;

//      case StoreType.BIGQUERY:
//        BigQueryConfig bqConfig = sinkStorageSpec.getBigqueryConfig();
//        String projectId = bqConfig.getProjectId();
//        String datasetId = bqConfig.getDatasetId();
//        String tableId = String
//            .format("%s:%s", featureSetSpec.getName(), featureSetSpec.getVersion());
//        String tableSpec = String.format("%s:%s.%s", projectId, datasetId, tableId);
//
//        input
//            .apply(
//                "Create BigQuery TableRow from FeatureRow",
//                ParDo.of(
//                    new FeatureRowToBigQueryTableRowDoFn(
//                        featureSpecByFeatureId, importJobSpecs.getJobId())))
//            .apply(
//                BigQueryIO.writeTableRows()
//                    .to(tableSpec)
//                    .withCreateDisposition(CreateDisposition.CREATE_NEVER)
//                    .withWriteDisposition(WRITE_APPEND)
//                    .withTimePartitioning(
//                        new TimePartitioning().setType("DAY").setField("event_timestamp")));
//        break;

      default:
        log.warn(
            "sinkStorageSpec of type '{}' is not supported, no FeatureRows will be written.",
            sinkStorageSpec.getType());
        break;
    }
    return PDone.in(input.getPipeline());
  }
}
