package feast.core.http;

import static feast.specs.FeatureSpecProto.FeatureSpec;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import feast.core.UIServiceProto.UIServiceTypes.EntityDetail;
import feast.core.UIServiceProto.UIServiceTypes.FeatureDetail;
import feast.core.UIServiceProto.UIServiceTypes.FeatureGroupDetail;
import feast.core.UIServiceProto.UIServiceTypes.StorageDetail;
import feast.core.config.StorageConfig.StorageSpecs;
import feast.core.model.EntityInfo;
import feast.core.model.FeatureGroupInfo;
import feast.core.model.FeatureInfo;
import feast.core.model.StorageInfo;
import feast.core.service.JobManagementService;
import feast.core.service.SpecService;
import java.util.Collections;
import org.junit.Before;
import org.junit.Test;

public class UiServiceControllerTest {

  private UiServiceController goodUiServiceController;
  private UiServiceController badUiServiceController;

  @Before
  public void setUp() throws Exception {
    StorageSpecs storageSpecs = StorageSpecs.builder().build();

    FeatureInfo mockFeatureInfo = mock(FeatureInfo.class);
    when(mockFeatureInfo.getFeatureDetail(storageSpecs))
        .thenReturn(FeatureDetail.getDefaultInstance());
    when(mockFeatureInfo.getFeatureSpec()).thenReturn(FeatureSpec.getDefaultInstance());
    when(mockFeatureInfo.resolve()).thenReturn(mockFeatureInfo);

    EntityInfo mockEntityInfo = mock(EntityInfo.class);
    when(mockEntityInfo.getEntityDetail()).thenReturn(EntityDetail.getDefaultInstance());

    FeatureGroupInfo mockFeatureGroupInfo = mock(FeatureGroupInfo.class);
    when(mockFeatureGroupInfo.getFeatureGroupDetail())
        .thenReturn(FeatureGroupDetail.getDefaultInstance());

    StorageInfo mockStorageInfo = mock(StorageInfo.class);
    when(mockStorageInfo.getStorageDetail()).thenReturn(StorageDetail.getDefaultInstance());

    SpecService goodMockSpecService = mock(SpecService.class);
    when(goodMockSpecService.getStorageSpecs()).thenReturn(storageSpecs);
    when(goodMockSpecService.listFeatures()).thenReturn(Collections.singletonList(mockFeatureInfo));
    when(goodMockSpecService.getFeatures(Collections.singletonList("1")))
        .thenReturn(Collections.singletonList(mockFeatureInfo));
    when(goodMockSpecService.listFeatureGroups())
        .thenReturn(Collections.singletonList(mockFeatureGroupInfo));
    when(goodMockSpecService.getFeatureGroups(Collections.singletonList("1")))
        .thenReturn(Collections.singletonList(mockFeatureGroupInfo));
    when(goodMockSpecService.listEntities()).thenReturn(Collections.singletonList(mockEntityInfo));
    when(goodMockSpecService.getEntities(Collections.singletonList("1")))
        .thenReturn(Collections.singletonList(mockEntityInfo));
    when(goodMockSpecService.listStorage()).thenReturn(Collections.singletonList(mockStorageInfo));
    when(goodMockSpecService.getStorage(Collections.singletonList("1")))
        .thenReturn(Collections.singletonList(mockStorageInfo));

    JobManagementService goodMockJobMangementService = mock(JobManagementService.class);

    goodUiServiceController =
        new UiServiceController(goodMockSpecService, goodMockJobMangementService);

    SpecService badMockSpecService = mock(SpecService.class);
    when(badMockSpecService.listFeatures()).thenReturn(null);
    when(badMockSpecService.getFeatures(Collections.singletonList("1"))).thenReturn(null);
    when(badMockSpecService.listFeatureGroups()).thenReturn(null);
    when(badMockSpecService.getFeatureGroups(Collections.singletonList("1"))).thenReturn(null);
    when(badMockSpecService.listEntities()).thenReturn(null);
    when(badMockSpecService.getEntities(Collections.singletonList("1"))).thenReturn(null);
    when(badMockSpecService.listStorage()).thenReturn(null);
    when(badMockSpecService.getStorage(Collections.singletonList("1"))).thenReturn(null);

    JobManagementService badMockJobMangementService = mock(JobManagementService.class);

    badUiServiceController =
        new UiServiceController(badMockSpecService, badMockJobMangementService);
  }

  @Test
  public void listFeatures() {
    FeatureDetail expected = FeatureDetail.getDefaultInstance();
    FeatureDetail actual = goodUiServiceController.listFeatures().getFeaturesList().get(0);
    assertEquals(expected, actual);
  }

  @Test(expected = Exception.class)
  public void listFeaturesWithException() {
    badUiServiceController.listFeatures();
  }

  @Test
  public void getFeature() {
    FeatureDetail expected = FeatureDetail.getDefaultInstance();
    FeatureDetail actual = goodUiServiceController.getFeature("1").getFeature();
    assertEquals(expected, actual);
  }

  @Test(expected = Exception.class)
  public void getFeatureWithException() {
    badUiServiceController.getFeature("1");
  }

  @Test
  public void listFeatureGroups() {
    FeatureGroupDetail expected = FeatureGroupDetail.getDefaultInstance();
    FeatureGroupDetail actual = goodUiServiceController.listFeatureGroups().getFeatureGroups(0);
    assertEquals(expected, actual);
  }

  @Test(expected = Exception.class)
  public void listFeatureGroupsWithException() {
    badUiServiceController.listFeatureGroups();
  }

  @Test
  public void getFeatureGroup() {
    FeatureGroupDetail expected = FeatureGroupDetail.getDefaultInstance();
    FeatureGroupDetail actual = goodUiServiceController.getFeatureGroup("1").getFeatureGroup();
    assertEquals(expected, actual);
  }

  @Test(expected = Exception.class)
  public void getFeatureGroupWithException() {
    badUiServiceController.getFeatureGroup("1");
  }

  @Test
  public void listEntities() {
    EntityDetail expected = EntityDetail.getDefaultInstance();
    EntityDetail actual = goodUiServiceController.listEntities().getEntitiesList().get(0);
    assertEquals(expected, actual);
  }

  @Test(expected = Exception.class)
  public void listEntitiesWithException() {
    badUiServiceController.listEntities();
  }

  @Test
  public void getEntity() {
    EntityDetail expected = EntityDetail.getDefaultInstance();
    EntityDetail actual = goodUiServiceController.getEntity("1").getEntity();
    assertEquals(expected, actual);
  }

  @Test(expected = Exception.class)
  public void getEntityWithException() {
    badUiServiceController.getEntity("1");
  }

  @Test
  public void listStorage() {
    StorageDetail expected = StorageDetail.getDefaultInstance();
    StorageDetail actual = goodUiServiceController.listStorage().getStorage(0);
    assertEquals(expected, actual);
  }

  @Test(expected = Exception.class)
  public void listStorageWithException() {
    badUiServiceController.listStorage();
  }

  @Test
  public void getStorage() {
    StorageDetail expected = StorageDetail.getDefaultInstance();
    StorageDetail actual = goodUiServiceController.getStorage("1").getStorage();
    assertEquals(expected, actual);
  }

  @Test(expected = Exception.class)
  public void getStorageWithException() {
    badUiServiceController.getStorage("1");
  }
}
