package feast.core.job.flink;

import java.net.URI;
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.client.RestTemplate;

@Slf4j
public class FlinkRestApi {

  private static final String SCHEME = "http";
  private static final String JOB_OVERVIEW_PATH = "jobs/overview";
  private final RestTemplate restTemplate;
  private final URI jobsOverviewUri;

  public FlinkRestApi(RestTemplate restTemplate, String masterUrl) throws Exception {
    this.restTemplate = restTemplate;
    this.jobsOverviewUri =
        new URI(String.format("%s://%s/%s", SCHEME, masterUrl, JOB_OVERVIEW_PATH));
  }

  public FlinkJobList getJobsOverview() {
    try {
      FlinkJobList jobList =  restTemplate.getForObject(jobsOverviewUri, FlinkJobList.class);
      if (jobList == null || jobList.getJobs() == null) {
        jobList.setJobs(Collections.emptyList());
      }
      return jobList;
    } catch (Exception e) {
      log.error("Unable to get job overview from {}: ", jobsOverviewUri, e);
      FlinkJobList flinkJobList = new FlinkJobList();
      flinkJobList.setJobs(Collections.emptyList());
      return flinkJobList;
    }
  }
}
