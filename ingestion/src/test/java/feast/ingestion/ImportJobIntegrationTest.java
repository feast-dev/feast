package feast.ingestion;

import feast.ingestion.options.ImportJobPipelineOptions;
import java.io.IOException;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;

public class ImportJobIntegrationTest {
  @Test
  public void run() throws IOException {

    ImportJobPipelineOptions pipelineOptions =
        PipelineOptionsFactory.create().as(ImportJobPipelineOptions.class);


    ImportJob.runPipeline(pipelineOptions);
  }
}
