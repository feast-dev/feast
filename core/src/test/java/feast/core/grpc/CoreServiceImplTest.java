package feast.core.grpc;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.CoreServiceProto.ApplyFeatureSetRequest;
import feast.core.CoreServiceProto.ApplyFeatureSetResponse;
import feast.core.CoreServiceProto.ApplyFeatureSetResponse.Status;
import feast.core.CoreServiceProto.ListFeatureSetsRequest;
import feast.core.CoreServiceProto.ListFeatureSetsResponse;
import feast.core.CoreServiceProto.ListStoresRequest.Filter;
import feast.core.CoreServiceProto.ListStoresResponse;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.SourceProto.KafkaSourceConfig;
import feast.core.SourceProto.Source;
import feast.core.SourceProto.SourceType;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.RedisConfig;
import feast.core.StoreProto.Store.StoreType;
import feast.core.StoreProto.Store.Subscription;
import feast.core.service.JobCoordinatorService;
import feast.core.service.SpecService;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Captor;
import org.mockito.Mock;

public class CoreServiceImplTest {

  @Mock
  private JobCoordinatorService jobCoordinatorService;

  @Mock
  private SpecService specService;

  @Captor
  private ArgumentCaptor<ArrayList<FeatureSetSpec>> fsListArgCaptor;

  @Before
  public void setUp() {
    initMocks(this);
  }

  @Test
  public void shouldPassCorrectListOfFeatureSetsToJobService()
      throws InvalidProtocolBufferException {
    CoreServiceImpl coreService = new CoreServiceImpl(specService, jobCoordinatorService);
    Store store = Store.newBuilder()
        .setType(StoreType.REDIS)
        .setRedisConfig(RedisConfig.newBuilder()
            .setHost("localhost").setPort(6379).build())
        .addSubscriptions(Subscription.newBuilder().setName("*").setVersion(">0"))
        .build();
    FeatureSetSpec fs1Sc1 = FeatureSetSpec.newBuilder()
        .setName("feature_set")
        .setVersion(1)
        .setSource(Source.newBuilder()
            .setType(SourceType.KAFKA)
            .setKafkaSourceConfig(
                KafkaSourceConfig.newBuilder()
                    .setBootstrapServers("kafka:9092")
                    .setTopic("topic1")
                    .build()))
        .build();
    FeatureSetSpec fs2Sc1 = FeatureSetSpec.newBuilder()
        .setName("feature_set_other")
        .setVersion(1)
        .setSource(Source.newBuilder()
            .setType(SourceType.KAFKA)
            .setKafkaSourceConfig(
                KafkaSourceConfig.newBuilder()
                    .setBootstrapServers("kafka:9092")
                    .setTopic("topic1")
                    .build()))
        .build();
    FeatureSetSpec fs3Sc2 = FeatureSetSpec.newBuilder()
        .setName("feature_set")
        .setVersion(2)
        .setSource(Source.newBuilder()
            .setType(SourceType.KAFKA)
            .setKafkaSourceConfig(
                KafkaSourceConfig.newBuilder()
                    .setBootstrapServers("kafka:9092")
                    .setTopic("topic2")
                    .build()))
        .build();
    when(specService.applyFeatureSet(fs1Sc1))
        .thenReturn(ApplyFeatureSetResponse.newBuilder()
            .setStatus(Status.CREATED)
            .setFeatureSet(fs1Sc1)
            .build());
    when(specService.listStores(ArgumentMatchers.any()))
        .thenReturn(ListStoresResponse.newBuilder()
            .addStore(store).build());
    when(specService.listFeatureSets(
        ListFeatureSetsRequest.Filter
            .newBuilder()
            .setFeatureSetName("*")
            .setFeatureSetVersion(">0").build()))
        .thenReturn(ListFeatureSetsResponse.newBuilder()
            .addFeatureSets(fs1Sc1)
            .addFeatureSets(fs3Sc2)
            .addFeatureSets(fs2Sc1).build());

    coreService.applyFeatureSet(ApplyFeatureSetRequest.newBuilder()
        .setFeatureSet(fs1Sc1).build(), new StreamObserver<ApplyFeatureSetResponse>() {
      @Override
      public void onNext(ApplyFeatureSetResponse applyFeatureSetResponse) {
      }

      @Override
      public void onError(Throwable throwable) {
      }

      @Override
      public void onCompleted() {
      }
    });

    verify(jobCoordinatorService, times(1))
        .startOrUpdateJob(fsListArgCaptor.capture(), eq(fs1Sc1.getSource()), eq(store));

    assertThat(fsListArgCaptor.getValue(), containsInAnyOrder(fs1Sc1, fs2Sc1));
  }


}