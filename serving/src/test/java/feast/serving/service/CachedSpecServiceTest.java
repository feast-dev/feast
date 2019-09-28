package feast.serving.service;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.collect.Lists;
import feast.core.CoreServiceProto.GetFeatureSetsRequest;
import feast.core.CoreServiceProto.GetFeatureSetsResponse;
import feast.core.CoreServiceProto.GetStoresRequest;
import feast.core.CoreServiceProto.GetStoresRequest.Filter;
import feast.core.CoreServiceProto.GetStoresResponse;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.Subscription;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

public class CachedSpecServiceTest {

  private static final String STORE_ID = "SERVING";

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  SpecService coreService;

  private Map<String, FeatureSetSpec> featureSetSpecs;
  private Store store;
  private CachedSpecService cachedSpecService;

  @Before
  public void setUp() {
    initMocks(this);

    featureSetSpecs = new LinkedHashMap<>();
    featureSetSpecs.put("fs1:1", FeatureSetSpec.newBuilder().setName("fs1").setVersion(1).build());
    featureSetSpecs.put("fs1:2", FeatureSetSpec.newBuilder().setName("fs1").setVersion(2).build());
    featureSetSpecs.put("fs1:3", FeatureSetSpec.newBuilder().setName("fs1").setVersion(3).build());
    featureSetSpecs.put("fs2:1", FeatureSetSpec.newBuilder().setName("fs2").setVersion(1).build());

    store = Store.newBuilder().setName(STORE_ID)
        .addSubscriptions(Subscription.newBuilder().setName("fs1").setVersion(">0").build())
        .addSubscriptions(Subscription.newBuilder().setName("fs2").setVersion(">0").build())
        .build();

    when(coreService.getStores(
        GetStoresRequest.newBuilder().setFilter(Filter.newBuilder().setName(STORE_ID)).build()))
        .thenReturn(GetStoresResponse.newBuilder().addStore(store).build());

    List<FeatureSetSpec> fs1FeatureSets = Lists
        .newArrayList(featureSetSpecs.get("fs1:1"), featureSetSpecs.get("fs1:2"),
            featureSetSpecs.get("fs1:3"));
    List<FeatureSetSpec> fs2FeatureSets = Lists.newArrayList(featureSetSpecs.get("fs2:1"));
    when(coreService.getFeatureSets(GetFeatureSetsRequest
        .newBuilder()
        .setFilter(GetFeatureSetsRequest.Filter.newBuilder().setFeatureSetName("fs1")
            .setFeatureSetVersion(">0").build())
        .build()))
        .thenReturn(GetFeatureSetsResponse.newBuilder().addAllFeatureSets(fs1FeatureSets).build());
    when(coreService.getFeatureSets(GetFeatureSetsRequest
        .newBuilder()
        .setFilter(GetFeatureSetsRequest.Filter.newBuilder().setFeatureSetName("fs2")
            .setFeatureSetVersion(">0").build())
        .build()))
        .thenReturn(GetFeatureSetsResponse.newBuilder().addAllFeatureSets(fs2FeatureSets).build());

    cachedSpecService = new CachedSpecService(coreService, STORE_ID);
  }

  @Test
  public void shouldPopulateAndReturnStore() {
    cachedSpecService.populateCache();
    Store actual = cachedSpecService.getStores(GetStoresRequest.newBuilder()
        .setFilter(Filter.newBuilder().setName(STORE_ID))
        .build()).getStore(0);
    assertThat(actual, equalTo(store));
  }

  @Test
  public void shouldPopulateAndReturnFeatureSets() {
    cachedSpecService.populateCache();
    GetFeatureSetsResponse actual = cachedSpecService
        .getFeatureSets(GetFeatureSetsRequest.newBuilder().build());

    assertThat(actual.getFeatureSetsList().size(), equalTo(featureSetSpecs.values().size()));
    for (FeatureSetSpec fs : featureSetSpecs.values()) {
      assertThat(actual.getFeatureSetsList(), hasItems(fs));
    }
  }
}
