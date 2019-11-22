package feast.core.job.direct;

import java.io.IOException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.beam.sdk.PipelineResult;

@Getter
@AllArgsConstructor
public class DirectJob {

  private String jobId;
  private PipelineResult pipelineResult;

  /**
   * Abort the job, if the state is not terminal. If the job has already concluded, this method will
   * do nothing.
   */
  public void abort() throws IOException {
    if (!pipelineResult.getState().isTerminal()) {
      pipelineResult.cancel();
    }
  }
}
