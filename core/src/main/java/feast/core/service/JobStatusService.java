package feast.core.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class JobStatusService {
  //
  //  private JobInfoRepository jobInfoRepository;
  //  private MetricsRepository metricsRepository;
  //
  //  @Autowired
  //  public JobStatusService(
  //      JobInfoRepository jobInfoRepository,
  //      MetricsRepository metricsRepository) {
  //    this.jobInfoRepository = jobInfoRepository;
  //    this.metricsRepository = metricsRepository;
  //  }
  //
  //  /**
  //   * Lists all jobs registered to the db, sorted by provided <code>orderBy</code>
  //   *
  //   * @param orderBy list order
  //   * @return list of JobDetails
  //   */
  //  @Transactional
  //  public List<JobDetail> listJobs(Sort orderBy) {
  //    List<JobInfo> jobs = jobInfoRepository.findAll(orderBy);
  //    return jobs.stream().map(JobInfo::getJobDetail).collect(Collectors.toList());
  //  }
  //
  //  /**
  //   * Lists all jobs registered to the db, sorted chronologically by creation time
  //   *
  //   * @return list of JobDetails
  //   */
  //  @Transactional
  //  public List<JobDetail> listJobs() {
  //    return listJobs(Sort.by(Sort.Direction.ASC, "created"));
  //  }
  //
  //  /**
  //   * Gets information regarding a single job.
  //   *
  //   * @param id feast-internal job id
  //   * @return JobDetail for that job
  //   */
  //  @Transactional
  //  public JobDetail getJob(String id) {
  //    Optional<JobInfo> job = jobInfoRepository.findById(id);
  //    if (!job.isPresent()) {
  //      throw new RetrievalException(Strings.lenientFormat("Unable to retrieve job with id %s",
  // id));
  //    }
  //    JobDetail.Builder jobDetailBuilder = job.get().getJobDetail().toBuilder();
  //    List<Metrics> metrics = metricsRepository.findByJobInfo_Id(id);
  //    for (Metrics metric : metrics) {
  //      jobDetailBuilder.putMetrics(metric.getName(), metric.getValue());
  //    }
  //    return jobDetailBuilder.build();
  //  }

}
