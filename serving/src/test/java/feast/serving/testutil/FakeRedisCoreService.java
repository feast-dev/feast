package feast.serving.testutil;

import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.RedisConfig;
import feast.core.StoreProto.Store.StoreType;
import feast.core.StoreProto.Store.Subscription;
import feast.serving.service.SpecStorage;
import java.util.List;
import java.util.Map;

public class FakeRedisCoreService implements SpecStorage {

  private static final String REDIS_HOST = "localhost";
  private static final int REDIS_PORT = 6379;

  @Override
  public Store getStoreDetails(String id) {
    return Store.newBuilder().setName(id).setType(StoreType.REDIS)
        .setRedisConfig(RedisConfig.newBuilder().setHost(REDIS_HOST).setPort(REDIS_PORT).build())
        .build();
  }

  @Override
  public Map<String, FeatureSetSpec> getFeatureSetSpecs(List<Subscription> subscriptions) {
    return null;
  }

  @Override
  public boolean isConnected() {
    return true;
  }
}
