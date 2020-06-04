package feast.databricks.types;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class RunNowRequest {
    private int job_id;
    private SparkSubmitParams spark_submit_params;
    private JarParams jar_params;
}
