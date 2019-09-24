package feast.serving.service;

import static org.junit.jupiter.api.Assertions.*;

import com.google.cloud.bigquery.BigQuery;
import feast.serving.ServingAPIProto.FeastServingType;
import feast.serving.service.serving.BigQueryServingService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

class BigQueryGrpcServingServiceTest {
  @Mock private BigQuery bigquery;

  private BigQueryServingService serving;

  @SuppressWarnings("WeakerAccess")
  @BeforeEach
  public void init() {
    this.serving = new BigQueryServingService(this.bigquery);
  }

  @Test
  void getFeastServingType() {
    assertEquals(
        FeastServingType.FEAST_SERVING_TYPE_OFFLINE, serving.getFeastServingType().getType());
  }

  @Test
  void getOnlineFeatures() {
    assertThrows(UnsupportedOperationException.class, () -> serving.getOnlineFeatures(null));
  }

  @Test
  void getBatchFeatures() {}

  @Test
  void getBatchFeaturesJobStatus() {}

  @Test
  void getBatchFeaturesJobUploadUrl() {}

  @Test
  void setBatchFeaturesJobUploadComplete() {}
}
