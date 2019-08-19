package feast.ingestion;

import com.google.cloud.bigquery.BigQueryOptions;
import feast.ingestion.options.ImportJobPipelineOptions;
import feast.ingestion.transform.ReadFeaturesTransform;
import feast.ingestion.transform.ToFeatureRowExtended;
import feast.ingestion.transform.WriteFeaturesTransform;
import feast.ingestion.util.ProtoUtil;
import feast.ingestion.util.StorageUtil;
import feast.specs.ImportJobSpecsProto.ImportJobSpecs;
import feast.specs.StorageSpecProto.StorageSpec;
import java.io.IOException;
import java.net.URISyntaxException;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

@SuppressWarnings("WeakerAccess")
@Slf4j
public class ImportJob {

  /**
   * Create and run a Beam pipeline from command line arguments.
   *
   * <p>The arguments will be passed to Beam {@code PipelineOptionsFactory} to create {@code
   * ImportJobPipelineOptions}.
   *
   * <p>The returned PipelineResult object can be used to check the state of the pipeline e.g. if it
   * is running, done or cancelled.
   *
   * @param args command line arguments, typically come from the main() method
   * @return PipelineResult
   * @throws IOException if importJobSpecsUri specified in args is not accessible
   */
  public static PipelineResult runPipeline(String[] args) throws IOException, URISyntaxException {
    ImportJobPipelineOptions pipelineOptions =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(ImportJobPipelineOptions.class);
    return runPipeline(pipelineOptions);
  }

  /**
   * Create and run a Beam pipeline from {@code ImportJobPipelineOptions}.
   *
   * <p>The returned PipelineResult object can be used to check the state of the pipeline e.g. if it
   * is running, done or cancelled.
   *
   * @param pipelineOptions configuration for the pipeline
   * @return PipelineResult
   * @throws IOException if importJobSpecsUri is not accessible
   */
  public static PipelineResult runPipeline(ImportJobPipelineOptions pipelineOptions)
      throws IOException, URISyntaxException {
    ImportJobSpecs importJobSpecs =
        ProtoUtil.createProtoMessageFromYamlFileUri(
            pipelineOptions.getImportJobSpecUri(),
            ImportJobSpecs.newBuilder(),
            ImportJobSpecs.class);
    pipelineOptions.setJobName(importJobSpecs.getJobId());
    setupStorage(importJobSpecs);
    Pipeline pipeline = Pipeline.create(pipelineOptions);
    pipeline
        .apply("Read FeatureRow", new ReadFeaturesTransform(importJobSpecs))
        .apply("Create FeatureRowExtended from FeatureRow", new ToFeatureRowExtended())
        .apply("Write FeatureRowExtended", new WriteFeaturesTransform(importJobSpecs));
    return pipeline.run();
  }

  /**
   * Configures the storage for Feast.
   *
   * <p>This method ensures that the storage backend is running and accessible by the import job,
   * and it also ensures that the storage backend has the necessary schema and configuration so the
   * import job can start writing Feature Row.
   *
   * <p>For example, when using BigQuery as the storage backend, this method ensures that, given a
   * list of features, the corresponding BigQuery dataset and table are created.
   *
   * @param importJobSpecs import job specification, refer to {@code ImportJobSpecs.proto}
   */
  private static void setupStorage(ImportJobSpecs importJobSpecs) {
    StorageSpec sinkStorageSpec = importJobSpecs.getSinkStorageSpec();
    String storageSpecType = sinkStorageSpec.getType();

    switch (storageSpecType) {
      case "BIGQUERY":
        StorageUtil.setupBigQuery(
            importJobSpecs.getSinkStorageSpec(),
            importJobSpecs.getEntitySpec(),
            importJobSpecs.getFeatureSpecsList(),
            BigQueryOptions.getDefaultInstance().getService());
        break;
      case "REDIS":
        StorageUtil.checkRedisConnection(sinkStorageSpec);
        break;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Unsupported type of sinkStorageSpec: \"%s\". Only REDIS and BIGQUERY are supported in Feast 0.2",
                storageSpecType));
    }
  }
}
