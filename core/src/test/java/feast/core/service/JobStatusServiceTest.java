package feast.core.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.collect.Lists;
import com.google.protobuf.Timestamp;
import feast.core.JobServiceProto.JobServiceTypes.JobDetail;
import feast.core.dao.JobInfoRepository;
import feast.core.dao.MetricsRepository;
import feast.core.exception.RetrievalException;
import feast.core.model.JobInfo;
import feast.core.model.JobStatus;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.springframework.data.domain.Sort;

public class JobStatusServiceTest {

  @Rule
  public final ExpectedException exception = ExpectedException.none();
  @Mock
  private JobInfoRepository jobInfoRepository;
  @Mock
  private MetricsRepository metricsRepository;

  @Before
  public void setUp() {
    initMocks(this);
  }

  @Test
  public void shouldListAllJobDetails() {
    JobInfo jobInfo1 =
        new JobInfo(
            "job1",
            "",
            "",
            "",
            "",
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            JobStatus.PENDING,
            "");
    jobInfo1.setCreated(Date.from(Instant.ofEpochSecond(1)));
    jobInfo1.setLastUpdated(Date.from(Instant.ofEpochSecond(1)));
    JobInfo jobInfo2 =
        new JobInfo(
            "job2",
            "",
            "",
            "",
            "",
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            JobStatus.PENDING,
            "");
    jobInfo2.setCreated(Date.from(Instant.ofEpochSecond(1)));
    jobInfo2.setLastUpdated(Date.from(Instant.ofEpochSecond(1)));
    when(jobInfoRepository.findAll(any(Sort.class)))
        .thenReturn(Lists.newArrayList(jobInfo1, jobInfo2));
    JobStatusService jobStatusService =
        new JobStatusService(jobInfoRepository, metricsRepository);
    List<JobDetail> actual = jobStatusService.listJobs();
    List<JobDetail> expected =
        Lists.newArrayList(
            JobDetail.newBuilder()
                .setId("job1")
                .setStatus("PENDING")
                .setCreated(Timestamp.newBuilder().setSeconds(1).build())
                .setLastUpdated(Timestamp.newBuilder().setSeconds(1).build())
                .build(),
            JobDetail.newBuilder()
                .setId("job2")
                .setStatus("PENDING")
                .setCreated(Timestamp.newBuilder().setSeconds(1).build())
                .setLastUpdated(Timestamp.newBuilder().setSeconds(1).build())
                .build());
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldReturnDetailOfRequestedJobId() {
    JobInfo jobInfo1 =
        new JobInfo(
            "job1",
            "",
            "",
            "",
            "",
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            JobStatus.PENDING,
            "");
    jobInfo1.setCreated(Date.from(Instant.ofEpochSecond(1)));
    jobInfo1.setLastUpdated(Date.from(Instant.ofEpochSecond(1)));
    when(jobInfoRepository.findById("job1")).thenReturn(Optional.of(jobInfo1));
    JobStatusService jobStatusService =
        new JobStatusService(jobInfoRepository, metricsRepository);
    JobDetail actual = jobStatusService.getJob("job1");
    JobDetail expected =
        JobDetail.newBuilder()
            .setId("job1")
            .setStatus("PENDING")
            .setCreated(Timestamp.newBuilder().setSeconds(1).build())
            .setLastUpdated(Timestamp.newBuilder().setSeconds(1).build())
            .build();
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldThrowErrorIfJobIdNotFoundWhenGettingJob() {
    when(jobInfoRepository.findById("job1")).thenReturn(Optional.empty());
    JobStatusService jobStatusService =
        new JobStatusService(jobInfoRepository, metricsRepository);
    exception.expect(RetrievalException.class);
    exception.expectMessage("Unable to retrieve job with id job1");
    jobStatusService.getJob("job1");
  }
}