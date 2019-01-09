package feast.core.job.flink;

import feast.core.job.JobManager;
import feast.specs.ImportSpecProto.ImportSpec;

public class FlinkJobManager implements JobManager {

  @Override
  public String submitJob(ImportSpec importSpec, String jobNamePrefix) {
    // TODO: implement submisssion job to flink
    return null;
  }

  @Override
  public void abortJob(String extId) {
    // TODO: implement aborting job
  }
}
