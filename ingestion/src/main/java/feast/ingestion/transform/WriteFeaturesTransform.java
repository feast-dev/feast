package feast.ingestion.transform;

import com.google.inject.Inject;
import feast.specs.ImportJobSpecsProto.ImportJobSpecs;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.store.serving.redis.FeatureRowToRedisMutationDoFn;
import feast.store.serving.redis.RedisCustomIO;
import feast.store.warehouse.bigquery.FeatureRowToBigQueryTableRowDoFn;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import java.util.HashMap;
import java.util.Map;

import static feast.specs.EntitySpecProto.EntitySpec;
import static feast.specs.FeatureSpecProto.FeatureSpec;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;

@Slf4j
public class WriteFeaturesTransform extends PTransform<PCollection<FeatureRowExtended>, PDone> {
  private ImportJobSpecs importJobSpecs;
  private String sinkStorageSpecType;
  private StorageSpec sinkStorageSpec;
  private EntitySpec entitySpec;
  private Map<String, FeatureSpec> featureSpecByFeatureId = new HashMap<>();

  @Inject
  public WriteFeaturesTransform(ImportJobSpecs importJobSpecs) {
    this.importJobSpecs = importJobSpecs;
    this.sinkStorageSpec = importJobSpecs.getSinkStorageSpec();
    this.sinkStorageSpecType = importJobSpecs.getSinkStorageSpec().getType();
    for (FeatureSpec featureSpec : importJobSpecs.getFeatureSpecsList()) {
      featureSpecByFeatureId.put(featureSpec.getId(), featureSpec);
    }
    this.entitySpec = importJobSpecs.getEntitySpec();
  }

  @Override
  public PDone expand(PCollection<FeatureRowExtended> input) {
    switch (sinkStorageSpecType) {
      case "REDIS":
        String redisHost = sinkStorageSpec.getOptionsOrThrow("host");
        int redisPort = Integer.parseInt(sinkStorageSpec.getOptionsOrDefault("port", "6379"));
        input
            .apply(
                "Create RedisMutation from FeatureRow",
                ParDo.of(new FeatureRowToRedisMutationDoFn(featureSpecByFeatureId)))
            .apply(RedisCustomIO.write(redisHost, redisPort));
        break;

      case "BIGQUERY":
        String projectId = sinkStorageSpec.getOptionsOrThrow("projectId");
        String datasetId = sinkStorageSpec.getOptionsOrThrow("datasetId");
        String tableId = entitySpec.getName();
        String tableSpec = String.format("%s:%s.%s", projectId, datasetId, tableId);

        input
            .apply(
                "Create BigQuery TableRow from FeatureRow",
                ParDo.of(
                    new FeatureRowToBigQueryTableRowDoFn(
                        featureSpecByFeatureId, importJobSpecs.getJobId())))
            .apply(
                BigQueryIO.writeTableRows()
                    .to(tableSpec)
                    .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                    .withWriteDisposition(WRITE_APPEND));
        break;

      default:
        log.warn(
            "sinkStorageSpec of type '{}' is not supported, no FeatureRows will be written.",
            sinkStorageSpecType);
        break;
    }
    return PDone.in(input.getPipeline());
  }
}
