package feast.core.grpc;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.Empty;
import feast.core.UIServiceGrpc;
import feast.core.UIServiceProto.UIServiceTypes.EntityDetail;
import feast.core.UIServiceProto.UIServiceTypes.FeatureDetail;
import feast.core.UIServiceProto.UIServiceTypes.FeatureGroupDetail;
import feast.core.UIServiceProto.UIServiceTypes.GetEntityRequest;
import feast.core.UIServiceProto.UIServiceTypes.GetEntityResponse;
import feast.core.UIServiceProto.UIServiceTypes.GetFeatureGroupRequest;
import feast.core.UIServiceProto.UIServiceTypes.GetFeatureGroupResponse;
import feast.core.UIServiceProto.UIServiceTypes.GetFeatureRequest;
import feast.core.UIServiceProto.UIServiceTypes.GetFeatureResponse;
import feast.core.UIServiceProto.UIServiceTypes.GetStorageRequest;
import feast.core.UIServiceProto.UIServiceTypes.GetStorageResponse;
import feast.core.UIServiceProto.UIServiceTypes.ListEntitiesResponse;
import feast.core.UIServiceProto.UIServiceTypes.ListFeatureGroupsResponse;
import feast.core.UIServiceProto.UIServiceTypes.ListFeaturesResponse;
import feast.core.UIServiceProto.UIServiceTypes.ListStorageResponse;
import feast.core.UIServiceProto.UIServiceTypes.StorageDetail;
import feast.core.model.EntityInfo;
import feast.core.model.FeatureGroupInfo;
import feast.core.model.FeatureInfo;
import feast.core.model.StorageInfo;
import feast.core.service.SpecService;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureGroupSpecProto.FeatureGroupSpec;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.StorageSpecProto.StorageSpec;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

public class UIServiceImplTest {

  @Mock public SpecService specService;

  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  @Rule public final ExpectedException expectedException = ExpectedException.none();

  private UIServiceGrpc.UIServiceBlockingStub client;

  @Before
  public void setUp() throws Exception {
    specService = mock(SpecService.class);

    UIServiceImpl service = new UIServiceImpl(specService);

    String serverName = InProcessServerBuilder.generateName();
    grpcCleanup.register(
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(service)
            .build()
            .start());

    client =
        UIServiceGrpc.newBlockingStub(
            grpcCleanup.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build()));
  }

  @Test
  public void getEntity_shouldReturnCorrectEntityDetail() {
    String entityName = "entity";
    GetEntityRequest req = GetEntityRequest.newBuilder().setId(entityName).build();

    EntitySpec entitySpec =
        EntitySpec.newBuilder().setName(entityName).setDescription("test entity").build();

    EntityInfo entityInfo = new EntityInfo(entitySpec);
    entityInfo.setLastUpdated(new Date());

    when(specService.getEntities(Collections.singletonList(entityName)))
        .thenReturn(Collections.singletonList(entityInfo));

    GetEntityResponse resp = client.getEntity(req);
    EntityDetail actual = resp.getEntity();

    assertThat(actual, equalTo(entityInfo.getEntityDetail()));
  }

  @Test
  @SuppressWarnings("ResultOfMethodCallIgnored")
  public void getEntity_shouldReturnClearErrorMessageForInvalidEntityName() {
    String entityName = "";
    GetEntityRequest req = GetEntityRequest.newBuilder().setId(entityName).build();

    EntitySpec entitySpec =
        EntitySpec.newBuilder().setName(entityName).setDescription("test entity").build();

    EntityInfo entityInfo = new EntityInfo(entitySpec);
    entityInfo.setLastUpdated(new Date());

    when(specService.getEntities(Collections.singletonList(entityName)))
        .thenThrow(new IllegalArgumentException("invalid entity name"));

    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage("Invalid entity name: " + entityName);
    client.getEntity(req);
  }

  @Test
  @SuppressWarnings("ResultOfMethodCallIgnored")
  public void getEntity_shouldReturnClearErrorMessageForAnyFailure() {
    String entityName = "";
    GetEntityRequest req = GetEntityRequest.newBuilder().setId(entityName).build();

    EntitySpec entitySpec =
        EntitySpec.newBuilder().setName(entityName).setDescription("test entity").build();

    EntityInfo entityInfo = new EntityInfo(entitySpec);
    entityInfo.setLastUpdated(new Date());

    when(specService.getEntities(Collections.singletonList(entityName)))
        .thenThrow(new RuntimeException());

    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage("Error while retrieving entity with name: " + entityName);
    client.getEntity(req);
  }

  @Test
  public void listEntities_shouldReturnAllEntities() {
    EntitySpec entitySpec1 =
        EntitySpec.newBuilder().setName("entity1").setDescription("test entity").build();
    EntityInfo entityInfo1 = new EntityInfo(entitySpec1);
    entityInfo1.setLastUpdated(new Date());

    EntitySpec entitySpec2 =
        EntitySpec.newBuilder().setName("entity2").setDescription("test entity").build();
    EntityInfo entityInfo2 = new EntityInfo(entitySpec2);
    entityInfo2.setLastUpdated(new Date());

    List<EntityInfo> entityInfos = Arrays.asList(entityInfo1, entityInfo2);

    when(specService.listEntities()).thenReturn(entityInfos);

    ListEntitiesResponse resp = client.listEntities(Empty.getDefaultInstance());
    List<EntityDetail> actual = resp.getEntitiesList();

    assertThat(
        actual,
        containsInAnyOrder(entityInfos.stream().map(EntityInfo::getEntityDetail).toArray()));
  }

  @Test
  @SuppressWarnings("ResultOfMethodCallIgnored")
  public void listEntities_shouldReturnClearErrorMessageForAnyFailure() {
    when(specService.listEntities()).thenThrow(new RuntimeException());

    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage("Error while getting all entities");

    client.listEntities(Empty.getDefaultInstance());
  }

  @Test
  public void getFeature_shouldReturnCorrectFeatureDetail() {
    String featureId = "entity.feature";
    FeatureInfo featureInfo = createFeatureInfo(featureId);

    when(specService.getFeatures(Collections.singletonList(featureId)))
        .thenReturn(Collections.singletonList(featureInfo));

    GetFeatureRequest req = GetFeatureRequest.newBuilder().setId(featureId).build();
    GetFeatureResponse resp = client.getFeature(req);

    FeatureDetail expected = featureInfo.getFeatureDetail();
    FeatureDetail actual = resp.getFeature();

    assertThat(actual, equalTo(expected));
  }

  @Test
  @SuppressWarnings("ResultOfMethodCallIgnored")
  public void getFeature_shouldReturnInvalidArgumentForInvalidFeatureId() {
    String featureId = "invalid.feature.id";

    when(specService.getFeatures(Collections.singletonList(featureId)))
        .thenThrow(new IllegalArgumentException());

    GetFeatureRequest req = GetFeatureRequest.newBuilder().setId(featureId).build();
    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage("Invalid feature ID: " + featureId);

    client.getFeature(req);
  }

  @Test
  @SuppressWarnings("ResultOfMethodCallIgnored")
  public void getFeature_shouldReturnErrorForAnyFailure() {
    String featureId = "invalid.feature.id";

    when(specService.getFeatures(Collections.singletonList(featureId)))
        .thenThrow(new RuntimeException());

    GetFeatureRequest req = GetFeatureRequest.newBuilder().setId(featureId).build();
    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage("Error while retrieving feature with ID: " + featureId);

    client.getFeature(req);
  }

  @Test
  public void listFeature_shouldReturnAllFeatures() {
    String featureId1 = "entity.feature1";
    String featureId2 = "entity.feature2";

    FeatureInfo featureInfo1 = createFeatureInfo(featureId1);
    FeatureInfo featureInfo2 = createFeatureInfo(featureId2);

    List<FeatureInfo> featureInfos = Arrays.asList(featureInfo1, featureInfo2);

    when(specService.listFeatures()).thenReturn(featureInfos);

    ListFeaturesResponse resp = client.listFeatures(Empty.getDefaultInstance());
    List<FeatureDetail> actual = resp.getFeaturesList();

    assertThat(
        actual,
        containsInAnyOrder(featureInfos.stream().map(FeatureInfo::getFeatureDetail).toArray()));
  }

  @Test
  @SuppressWarnings("ResultOfMethodCallIgnored")
  public void listFeature_shouldReturnClearErrorMessageForAnyFailure() {
    when(specService.listFeatures()).thenThrow(new RuntimeException());

    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage("Error while getting all features");

    client.listFeatures(Empty.getDefaultInstance());
  }

  @Test
  public void getFeatureGroup_shouldReturnCorrectFeatureGroup() {
    String featureGroupId = "featureGroup";

    FeatureGroupInfo featureGroupInfo = createFeatureGroupInfo(featureGroupId);

    when(specService.getFeatureGroups(Collections.singletonList(featureGroupId)))
        .thenReturn(Collections.singletonList(featureGroupInfo));

    GetFeatureGroupRequest req = GetFeatureGroupRequest.newBuilder().setId(featureGroupId).build();

    GetFeatureGroupResponse resp = client.getFeatureGroup(req);
    FeatureGroupDetail actual = resp.getFeatureGroup();

    assertThat(actual, equalTo(featureGroupInfo.getFeatureGroupDetail()));
  }

  @Test
  @SuppressWarnings("ResultOfMethodCallIgnored")
  public void getFeatureGroup_shouldReturnClearErrorMessageForInvalidId() {
    String invalidFeatureGroupId = "invalidId";

    when(specService.getFeatureGroups(Collections.singletonList(invalidFeatureGroupId)))
        .thenThrow(new IllegalArgumentException());

    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage("Invalid feature group ID: " + invalidFeatureGroupId);

    GetFeatureGroupRequest req =
        GetFeatureGroupRequest.newBuilder().setId(invalidFeatureGroupId).build();

    client.getFeatureGroup(req);
  }

  @Test
  @SuppressWarnings("ResultOfMethodCallIgnored")
  public void getFeatureGroup_shouldReturnClearErrorMessageForAnyFailure() {
    String featureGroupId = "invalidId";

    when(specService.getFeatureGroups(Collections.singletonList(featureGroupId)))
        .thenThrow(new RuntimeException());

    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage("Error while getting feature group with ID: " + featureGroupId);

    GetFeatureGroupRequest req = GetFeatureGroupRequest.newBuilder().setId(featureGroupId).build();

    client.getFeatureGroup(req);
  }

  @Test
  public void listFeatureGroup_shouldReturnAllFeatureGroups() {
    FeatureGroupInfo featureGroupInfo1 = createFeatureGroupInfo("featureGroup1");
    FeatureGroupInfo featureGroupInfo2 = createFeatureGroupInfo("featureGroup2");

    List<FeatureGroupInfo> featureGroupInfos = Arrays.asList(featureGroupInfo1, featureGroupInfo2);
    when(specService.listFeatureGroups()).thenReturn(featureGroupInfos);

    ListFeatureGroupsResponse resp = client.listFeatureGroups(Empty.getDefaultInstance());
    List<FeatureGroupDetail> actual = resp.getFeatureGroupsList();

    assertThat(
        actual,
        containsInAnyOrder(
            featureGroupInfos.stream().map(FeatureGroupInfo::getFeatureGroupDetail).toArray()));
  }

  @Test
  @SuppressWarnings("ResultOfMethodCallIgnored")
  public void listFeatureGroup_shouldReturnClearErrorMessageForAnyFailure() {
    when(specService.listFeatureGroups()).thenThrow(new RuntimeException());

    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage("Error while getting all feature groups");
    client.listFeatureGroups(Empty.getDefaultInstance());
  }

  @Test
  public void getStorage_shouldReturnCorrectStorageDetail() {
    String storageId = "mystorage";
    StorageSpec storageSpec = StorageSpec.newBuilder().setId(storageId).build();
    StorageInfo storageInfo = new StorageInfo(storageSpec);
    storageInfo.setLastUpdated(new Date());

    when(specService.getStorage(Collections.singletonList(storageId)))
        .thenReturn(Collections.singletonList(storageInfo));

    GetStorageRequest req = GetStorageRequest.newBuilder().setId(storageId).build();
    GetStorageResponse resp = client.getStorage(req);
    StorageDetail actual = resp.getStorage();

    assertThat(actual, equalTo(storageInfo.getStorageDetail()));
  }

  @Test
  @SuppressWarnings("ResultOfMethodCallIgnored")
  public void getStorage_shouldReturnErrorForInvalidStorageId() {
    String storageId = "invalid";

    when(specService.getStorage(Collections.singletonList(storageId)))
        .thenThrow(new IllegalArgumentException());

    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage("Invalid storage ID: " + storageId);

    GetStorageRequest req = GetStorageRequest.newBuilder().setId(storageId).build();
    client.getStorage(req);
  }

  @Test
  @SuppressWarnings("ResultOfMethodCallIgnored")
  public void getStorage_shouldReturnErrorForAnyFailure() {
    String storageId = "myStorage";

    when(specService.getStorage(Collections.singletonList(storageId)))
        .thenThrow(new RuntimeException());

    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage("Error while retrieving storage detail with ID: " + storageId);

    GetStorageRequest req = GetStorageRequest.newBuilder().setId(storageId).build();
    client.getStorage(req);
  }

  @Test
  public void listStorage_shouldReturnAllStorageDetail() {
    String storageId1 = "storage1";
    StorageSpec storageSpec1 = StorageSpec.newBuilder().setId(storageId1).build();
    StorageInfo storageInfo1 = new StorageInfo(storageSpec1);
    storageInfo1.setLastUpdated(new Date());

    String storageId2 = "storage2";
    StorageSpec storageSpec2 = StorageSpec.newBuilder().setId(storageId2).build();
    StorageInfo storageInfo2 = new StorageInfo(storageSpec2);
    storageInfo2.setLastUpdated(new Date());

    List<StorageInfo> storageInfos = Arrays.asList(storageInfo1, storageInfo2);

    when(specService.listStorage()).thenReturn(storageInfos);

    ListStorageResponse resp = client.listStorage(Empty.getDefaultInstance());
    List<StorageDetail> actual = resp.getStorageList();

    assertThat(
        actual,
        containsInAnyOrder(storageInfos.stream().map(StorageInfo::getStorageDetail).toArray()));
  }

  @Test
  @SuppressWarnings("ResultOfMethodCallIgnored")
  public void listStorage_shouldReturnErrorForAnyFailure() {
    when(specService.listStorage()).thenThrow(new RuntimeException());

    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage("Error while getting all storage details");

    client.listStorage(Empty.getDefaultInstance());
  }

  private FeatureInfo createFeatureInfo(String featureId) {
    StorageSpec warehouseSpec = StorageSpec.newBuilder().setId("warehouse").build();
    StorageSpec servingSpec = StorageSpec.newBuilder().setId("serving").build();
    EntitySpec entitySpec = EntitySpec.newBuilder().setName("entity").build();

    StorageInfo warehouseStoreInfo = new StorageInfo(warehouseSpec);
    StorageInfo servingStoreInfo = new StorageInfo(servingSpec);

    EntityInfo entityInfo = new EntityInfo(entitySpec);
    FeatureSpec featureSpec = FeatureSpec.newBuilder().setId(featureId).build();
    FeatureInfo featureInfo =
        new FeatureInfo(featureSpec, entityInfo, servingStoreInfo, warehouseStoreInfo, null);
    featureInfo.setCreated(new Date());
    featureInfo.setLastUpdated(new Date());
    return featureInfo;
  }

  private FeatureGroupInfo createFeatureGroupInfo(String featureGroupId) {
    StorageSpec warehouseSpec = StorageSpec.newBuilder().setId("warehouse").build();
    StorageSpec servingSpec = StorageSpec.newBuilder().setId("serving").build();

    StorageInfo warehouseStoreInfo = new StorageInfo(warehouseSpec);
    StorageInfo servingStoreInfo = new StorageInfo(servingSpec);

    FeatureGroupSpec featureGroupSpec = FeatureGroupSpec.newBuilder().setId(featureGroupId).build();
    FeatureGroupInfo featureGroupInfo =
        new FeatureGroupInfo(featureGroupSpec, servingStoreInfo, warehouseStoreInfo);
    featureGroupInfo.setCreated(new Date());
    featureGroupInfo.setLastUpdated(new Date());
    return featureGroupInfo;
  }
}
