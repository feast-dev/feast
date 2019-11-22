package feast.core.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.collect.Lists;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.SourceProto.KafkaSourceConfig;
import feast.core.SourceProto.SourceType;
import feast.core.StoreProto;
import feast.core.StoreProto.Store.RedisConfig;
import feast.core.StoreProto.Store.StoreType;
import feast.core.dao.JobInfoRepository;
import feast.core.job.JobManager;
import feast.core.job.Runner;
import feast.core.model.FeatureSet;
import feast.core.model.JobInfo;
import feast.core.model.JobStatus;
import feast.core.model.Source;
import feast.core.model.Store;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

public class JobCoordinatorServiceTest {

  @Rule public final ExpectedException exception = ExpectedException.none();
  @Mock JobInfoRepository jobInfoRepository;
  @Mock JobManager jobManager;

  private JobCoordinatorService jobCoordinatorService;
  private JobInfo existingJob;
  private Source defaultSource;

  @Before
  public void setUp() {
    initMocks(this);

    Store store =
        Store.fromProto(
            StoreProto.Store.newBuilder()
                .setName("SERVING")
                .setType(StoreType.REDIS)
                .setRedisConfig(RedisConfig.newBuilder().setHost("localhost").setPort(6379))
                .build());
    defaultSource =
        new Source(
            SourceType.KAFKA,
            KafkaSourceConfig.newBuilder()
                .setBootstrapServers("kafka:9092")
                .setTopic("feast-topic")
                .build(),
            true);
    FeatureSet featureSet1 = new FeatureSet();
    featureSet1.setId("featureSet1:1");
    featureSet1.setSource(defaultSource);
    FeatureSet featureSet2 = new FeatureSet();
    featureSet2.setId("featureSet2:1");
    featureSet2.setSource(defaultSource);
    existingJob =
        new JobInfo(
            "extid",
            "name",
            "DirectRunner",
            defaultSource,
            store,
            Lists.newArrayList(featureSet1, featureSet2),
            Lists.newArrayList(),
            JobStatus.RUNNING);
    when(jobInfoRepository.findBySourceIdAndStoreName(defaultSource.getId(), "SERVING"))
        .thenReturn(Lists.newArrayList(existingJob));

    jobCoordinatorService = new JobCoordinatorService(jobInfoRepository, jobManager);
    jobCoordinatorService = spy(jobCoordinatorService);
  }

  @Test
  public void shouldNotStartOrUpdateJobIfNoChanges() {
    FeatureSetSpec featureSet1 =
        FeatureSetSpec.newBuilder()
            .setName("featureSet1")
            .setVersion(1)
            .setSource(defaultSource.toProto())
            .build();
    FeatureSetSpec featureSet2 =
        FeatureSetSpec.newBuilder()
            .setName("featureSet2")
            .setVersion(1)
            .setSource(defaultSource.toProto())
            .build();
    StoreProto.Store store =
        StoreProto.Store.newBuilder()
            .setName("SERVING")
            .setType(StoreType.REDIS)
            .setRedisConfig(RedisConfig.newBuilder().setHost("localhost").setPort(6379))
            .build();
    JobInfo jobInfo =
        jobCoordinatorService.startOrUpdateJob(
            Lists.newArrayList(featureSet1, featureSet2), defaultSource.toProto(), store);
    assertThat(jobInfo, equalTo(existingJob));
  }

  @Test
  public void shouldStartJobIfNotExists() {
    FeatureSetSpec featureSet =
        FeatureSetSpec.newBuilder()
            .setName("featureSet")
            .setVersion(1)
            .setSource(defaultSource.toProto())
            .build();
    StoreProto.Store store =
        StoreProto.Store.newBuilder()
            .setName("SERVING")
            .setType(StoreType.REDIS)
            .setRedisConfig(RedisConfig.newBuilder().setHost("localhost").setPort(6379))
            .build();
    String jobId = "featureSet-to-SERVING";
    String extJobId = "extId123";
    when(jobCoordinatorService.createJobId("featureSet", "SERVING")).thenReturn(jobId);
    when(jobManager.startJob(jobId, Lists.newArrayList(featureSet), store)).thenReturn(extJobId);
    when(jobManager.getRunnerType()).thenReturn(Runner.DIRECT);
    FeatureSet expectedFeatureSet = new FeatureSet();
    expectedFeatureSet.setId("featureSet:1");
    JobInfo expectedJobInfo =
        new JobInfo(
            jobId,
            extJobId,
            "DirectRunner",
            defaultSource,
            Store.fromProto(store),
            Lists.newArrayList(expectedFeatureSet),
            JobStatus.RUNNING);
    when(jobInfoRepository.save(expectedJobInfo)).thenReturn(expectedJobInfo);
    JobInfo jobInfo =
        jobCoordinatorService.startOrUpdateJob(
            Lists.newArrayList(featureSet), defaultSource.toProto(), store);
    assertThat(jobInfo, equalTo(expectedJobInfo));
  }

  @Test
  public void shouldUpdateJobIfAlreadyExistsButThereIsAChange() {
    FeatureSetSpec featureSet =
        FeatureSetSpec.newBuilder()
            .setName("featureSet1")
            .setVersion(1)
            .setSource(defaultSource.toProto())
            .build();
    StoreProto.Store store =
        StoreProto.Store.newBuilder()
            .setName("SERVING")
            .setType(StoreType.REDIS)
            .setRedisConfig(RedisConfig.newBuilder().setHost("localhost").setPort(6379))
            .build();
    String extId = "extId123";
    JobInfo modifiedJob =
        new JobInfo(
            existingJob.getId(),
            existingJob.getExtId(),
            existingJob.getRunner(),
            defaultSource,
            Store.fromProto(store),
            Lists.newArrayList(FeatureSet.fromProto(featureSet)),
            JobStatus.RUNNING);
    when(jobManager.updateJob(modifiedJob)).thenReturn(extId);
    JobInfo expectedJobInfo = modifiedJob;
    expectedJobInfo.setExtId(extId);
    when(jobInfoRepository.save(expectedJobInfo)).thenReturn(expectedJobInfo);
    JobInfo jobInfo =
        jobCoordinatorService.startOrUpdateJob(
            Lists.newArrayList(featureSet), defaultSource.toProto(), store);
    assertThat(jobInfo, equalTo(expectedJobInfo));
  }
}
