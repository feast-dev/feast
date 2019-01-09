package feast.core.job.direct;

import feast.core.job.JobManager;
import feast.specs.ImportSpecProto.ImportSpec;

public class DirectRunnerJobManager implements JobManager {

  @Override
  public String submitJob(ImportSpec importSpec, String jobNamePrefix) {
    // TODO: implement job submission to direct runner
    return null;
  }

  @Override
  public void abortJob(String extId) {
    throw new UnsupportedOperationException("Unable to abort a job running in direct runner");
  }
}
