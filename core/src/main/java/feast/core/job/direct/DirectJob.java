package feast.core.job.direct;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.ExecuteWatchdog;


@Getter
@AllArgsConstructor
public class DirectJob {
  private String jobId;
  private ExecuteWatchdog watchdog;
  private DefaultExecuteResultHandler resultHandler;
}
