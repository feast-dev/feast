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
import feast.core.DatasetServiceGrpc;
import feast.core.DatasetServiceProto.DatasetInfo;
import feast.core.DatasetServiceProto.FeatureSet;
import feast.core.DatasetServiceProto.DatasetServiceTypes.CreateDatasetRequest;
import feast.core.DatasetServiceProto.DatasetServiceTypes.CreateDatasetResponse;
import feast.core.training.BigQueryDatasetCreator;
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

public class DatasetServiceImplTest {

  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  @Rule public final ExpectedException expectedException = ExpectedException.none();

  @Mock private BigQueryDatasetCreator trainingDatasetCreator;
  private DatasetServiceGrpc.DatasetServiceBlockingStub client;

  private Timestamp validStartDate;
  private Timestamp validEndDate;
  private FeatureSet validFeatureSet;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    DatasetServiceImpl DatasetService = new DatasetServiceImpl(trainingDatasetCreator);
    String serverName = InProcessServerBuilder.generateName();

    grpcCleanup.register(
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(DatasetService)
            .build()
            .start());

    client =
        DatasetServiceGrpc.newBlockingStub(
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
  public void shouldCallcreateDatasetWithCorrectRequest() {
    DatasetInfo datasetInfo =
        DatasetInfo.newBuilder().setName("mydataset").setTableUrl("project.dataset.table").build();
    when(trainingDatasetCreator.createDataset(
            any(FeatureSet.class),
            any(Timestamp.class),
            any(Timestamp.class),
            anyLong(),
            anyString()))
        .thenReturn(datasetInfo);

    long limit = 9999;
    String namePrefix = "mydataset";
    CreateDatasetRequest request =
        CreateDatasetRequest.newBuilder()
            .setFeatureSet(validFeatureSet)
            .setStartDate(validStartDate)
            .setEndDate(validEndDate)
            .setLimit(limit)
            .setNamePrefix(namePrefix)
            .build();

    client.createDataset(request);

    verify(trainingDatasetCreator)
        .createDataset(validFeatureSet, validStartDate, validEndDate, limit, namePrefix);
  }

  @Test
  public void shouldPropagateCreatedDatasetInfo() {
    DatasetInfo datasetInfo =
        DatasetInfo.newBuilder().setName("mydataset").setTableUrl("project.dataset.table").build();
    when(trainingDatasetCreator.createDataset(
            any(FeatureSet.class),
            any(Timestamp.class),
            any(Timestamp.class),
            anyLong(),
            anyString()))
        .thenReturn(datasetInfo);

    long limit = 9999;
    String namePrefix = "mydataset";
    CreateDatasetRequest request =
        CreateDatasetRequest.newBuilder()
            .setFeatureSet(validFeatureSet)
            .setStartDate(validEndDate)
            .setEndDate(validEndDate)
            .setLimit(limit)
            .setNamePrefix(namePrefix)
            .build();

    CreateDatasetResponse resp = client.createDataset(request);
    DatasetInfo actual = resp.getDatasetInfo();

    assertThat(actual, equalTo(datasetInfo));
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  public void shouldThrowExceptionIfFeatureSetEmpty() {
    FeatureSet emptyFeatureSet = FeatureSet.newBuilder().setEntityName("myentity").build();

    CreateDatasetRequest request =
        CreateDatasetRequest.newBuilder()
            .setFeatureSet(emptyFeatureSet)
            .setStartDate(validStartDate)
            .setEndDate(validEndDate)
            .build();

    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage("feature set is empty");

    client.createDataset(request);
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  public void shouldThrowExceptionIfFeatureSetHasEmptyEntity() {
    FeatureSet emptyFeatureSet = FeatureSet.newBuilder().setEntityName("").build();

    CreateDatasetRequest request =
        CreateDatasetRequest.newBuilder()
            .setFeatureSet(emptyFeatureSet)
            .setStartDate(validStartDate)
            .setEndDate(validEndDate)
            .build();

    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage("entity name in feature set is null or empty");

    client.createDataset(request);
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

    CreateDatasetRequest request =
        CreateDatasetRequest.newBuilder()
            .setFeatureSet(emptyFeatureSet)
            .setStartDate(validStartDate)
            .setEndDate(validEndDate)
            .build();

    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage("feature set contains different entity name: driver");

    client.createDataset(request);
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  public void shouldThrowExceptionWhenStartDateIsAfterEndDate() throws ParseException {
    Timestamp laterStartDate = Timestamps.parse("2020-12-01T10:00:20.021-05:00");

    CreateDatasetRequest request =
        CreateDatasetRequest.newBuilder()
            .setFeatureSet(validFeatureSet)
            .setStartDate(laterStartDate)
            .setEndDate(validEndDate)
            .build();

    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage("startDate is after endDate");

    client.createDataset(request);
  }
}
