package feast.core.grpc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import feast.core.TrainingServiceGrpc;
import feast.core.TrainingServiceProto.DatasetInfo;
import feast.core.TrainingServiceProto.FeatureSet;
import feast.core.TrainingServiceProto.TrainingServiceTypes.CreateTrainingDatasetRequest;
import feast.core.TrainingServiceProto.TrainingServiceTypes.CreateTrainingDatasetResponse;
import feast.core.training.BigQueryTrainingDatasetCreator;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import java.text.ParseException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TrainingServiceImplTest {

  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  @Rule public final ExpectedException expectedException = ExpectedException.none();

  @Mock private BigQueryTrainingDatasetCreator trainingDatasetCreator;
  private TrainingServiceGrpc.TrainingServiceBlockingStub client;

  private Timestamp validStartDate;
  private Timestamp validEndDate;
  private FeatureSet validFeatureSet;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    TrainingServiceImpl trainingService = new TrainingServiceImpl(trainingDatasetCreator);
    String serverName = InProcessServerBuilder.generateName();

    grpcCleanup.register(
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(trainingService)
            .build()
            .start());

    client =
        TrainingServiceGrpc.newBlockingStub(
            InProcessChannelBuilder.forName(serverName).directExecutor().build());

    validStartDate = Timestamps.parse("2018-01-02T10:00:20.021-05:00");
    validEndDate = Timestamps.parse("2018-12-01T10:00:20.021-05:00");
    validFeatureSet =
        FeatureSet.newBuilder()
            .setEntityName("myentity")
            .addFeatureIds("myentity.none.feature1")
            .addFeatureIds("myentity.second.feature2")
            .build();
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  public void shouldCallCreateTrainingDatasetWithCorrectRequest() {
    DatasetInfo datasetInfo =
        DatasetInfo.newBuilder().setName("mydataset").setTableUrl("project.dataset.table").build();
    when(trainingDatasetCreator.createTrainingDataset(
            any(FeatureSet.class),
            any(Timestamp.class),
            any(Timestamp.class),
            anyLong(),
            anyString()))
        .thenReturn(datasetInfo);

    long limit = 9999;
    String namePrefix = "mydataset";
    CreateTrainingDatasetRequest request =
        CreateTrainingDatasetRequest.newBuilder()
            .setFeatureSet(validFeatureSet)
            .setStartDate(validStartDate)
            .setEndDate(validEndDate)
            .setLimit(limit)
            .setNamePrefix(namePrefix)
            .build();

    client.createTrainingDataset(request);

    verify(trainingDatasetCreator)
        .createTrainingDataset(validFeatureSet, validStartDate, validEndDate, limit, namePrefix);
  }

  @Test
  public void shouldPropagateCreatedDatasetInfo() {
    DatasetInfo datasetInfo =
        DatasetInfo.newBuilder().setName("mydataset").setTableUrl("project.dataset.table").build();
    when(trainingDatasetCreator.createTrainingDataset(
            any(FeatureSet.class),
            any(Timestamp.class),
            any(Timestamp.class),
            anyLong(),
            anyString()))
        .thenReturn(datasetInfo);

    long limit = 9999;
    String namePrefix = "mydataset";
    CreateTrainingDatasetRequest request =
        CreateTrainingDatasetRequest.newBuilder()
            .setFeatureSet(validFeatureSet)
            .setStartDate(validEndDate)
            .setEndDate(validEndDate)
            .setLimit(limit)
            .setNamePrefix(namePrefix)
            .build();

    CreateTrainingDatasetResponse resp = client.createTrainingDataset(request);
    DatasetInfo actual = resp.getDatasetInfo();

    assertThat(actual, equalTo(datasetInfo));
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  public void shouldThrowExceptionIfFeatureSetEmpty() {
    FeatureSet emptyFeatureSet = FeatureSet.newBuilder().setEntityName("myentity").build();

    CreateTrainingDatasetRequest request =
        CreateTrainingDatasetRequest.newBuilder()
            .setFeatureSet(emptyFeatureSet)
            .setStartDate(validStartDate)
            .setEndDate(validEndDate)
            .build();

    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage("feature set is empty");

    client.createTrainingDataset(request);
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  public void shouldThrowExceptionIfFeatureSetHasEmptyEntity() {
    FeatureSet emptyFeatureSet = FeatureSet.newBuilder().setEntityName("").build();

    CreateTrainingDatasetRequest request =
        CreateTrainingDatasetRequest.newBuilder()
            .setFeatureSet(emptyFeatureSet)
            .setStartDate(validStartDate)
            .setEndDate(validEndDate)
            .build();

    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage("entity name in feature set is null or empty");

    client.createTrainingDataset(request);
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  public void shouldThrowExceptionIfFeatureSetHasDifferentEntity() {
    FeatureSet emptyFeatureSet =
        FeatureSet.newBuilder()
            .setEntityName("myentity")
            .addFeatureIds("myentity.none.feature1")
            .addFeatureIds("driver.none.feature2")
            .build();

    CreateTrainingDatasetRequest request =
        CreateTrainingDatasetRequest.newBuilder()
            .setFeatureSet(emptyFeatureSet)
            .setStartDate(validStartDate)
            .setEndDate(validEndDate)
            .build();

    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage("feature set contains different entity name: driver");

    client.createTrainingDataset(request);
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  public void shouldThrowExceptionWhenStartDateIsAfterEndDate() throws ParseException {
    Timestamp laterStartDate = Timestamps.parse("2020-12-01T10:00:20.021-05:00");

    CreateTrainingDatasetRequest request =
        CreateTrainingDatasetRequest.newBuilder()
            .setFeatureSet(validFeatureSet)
            .setStartDate(laterStartDate)
            .setEndDate(validEndDate)
            .build();

    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage("startDate is after endDate");

    client.createTrainingDataset(request);
  }
}
