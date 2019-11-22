package feast.ingestion.util;

import static feast.types.ValueProto.ValueType.Enum.INT32;
import static feast.types.ValueProto.ValueType.Enum.STRING_LIST;

import com.google.cloud.bigquery.BigQuery;
import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.ingestion.utils.StoreUtil;
import org.junit.Test;
import org.mockito.Mockito;

public class StoreUtilTest {
  @Test
  public void setupBigQuery_shouldCreateTable_givenFeatureSetSpec() {
    FeatureSetSpec featureSetSpec =
        FeatureSetSpec.newBuilder()
            .setName("feature_set_1")
            .setVersion(1)
            .addEntities(EntitySpec.newBuilder().setName("entity_1").setValueType(INT32))
            .addFeatures(FeatureSpec.newBuilder().setName("feature_1").setValueType(INT32))
            .addFeatures(FeatureSpec.newBuilder().setName("feature_2").setValueType(STRING_LIST))
            .build();
    BigQuery mockedBigquery = Mockito.mock(BigQuery.class);
    StoreUtil.setupBigQuery(featureSetSpec, "project-1", "dataset_1", mockedBigquery);
  }
}
