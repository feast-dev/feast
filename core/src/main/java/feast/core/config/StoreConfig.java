package feast.core.config;

import com.google.common.collect.Lists;
import feast.core.StoreProto;
import feast.core.StoreProto.Store.BigQueryConfig;
import feast.core.StoreProto.Store.Builder;
import feast.core.StoreProto.Store.CassandraConfig;
import feast.core.StoreProto.Store.RedisConfig;
import feast.core.StoreProto.Store.StoreType;
import feast.core.StoreProto.Store.Subscription;
import feast.core.dao.StoreRepository;
import feast.core.model.Store;
import feast.core.util.TypeConversion;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration stores installed at startup.
 */
@Configuration
@Slf4j
public class StoreConfig {

  @Autowired
  public void initStores(StoreRepository storeRepository,
      @Value("${feast.store.serving.type}") String servingType,
      @Value("${feast.store.serving.options}") String servingOptions,
      @Value("${feast.store.warehouse.type}") String warehouseType,
      @Value("${feast.store.warehouse.options}") String warehouseOptions) {
    Store servingStore = Store.fromProto(getStore("SERVING", servingType, servingOptions));
    Store warehouseStore = Store.fromProto(getStore("WAREHOUSE", warehouseType, warehouseOptions));
    storeRepository.saveAll(Lists.newArrayList(servingStore, warehouseStore));
  }

  private StoreProto.Store getStore(String name, String type, String options) {
    Map<String, String> optionsMap = TypeConversion
        .convertJsonStringToMap(options);

    List<Subscription> subscriptionList = new ArrayList<>();
    for (String sub : optionsMap.getOrDefault("subscriptions", "").split(",")) {
      String[] subSplit = sub.split(":");
      subscriptionList.add(Subscription.newBuilder()
          .setName(subSplit[0])
          .setVersion(subSplit[1])
          .build());
    }
    Builder servingStoreBuilder = StoreProto.Store.newBuilder()
        .setName(name)
        .setType(StoreType.valueOf(type))
        .addAllSubscriptions(subscriptionList);
    switch (servingStoreBuilder.getType()) {
      case REDIS:
        RedisConfig redisConfig = RedisConfig.newBuilder()
            .setHost(optionsMap.get("host"))
            .setPort(Integer.parseInt(optionsMap.get("port")))
            .build();
        return servingStoreBuilder.setRedisConfig(redisConfig).build();
      case BIGQUERY:
        BigQueryConfig bigQueryConfig = BigQueryConfig.newBuilder()
            .setProjectId(optionsMap.get("projectId"))
            .setDatasetId(optionsMap.get("datasetId"))
            .build();
        return servingStoreBuilder.setBigqueryConfig(bigQueryConfig).build();
      case CASSANDRA:
        CassandraConfig cassandraConfig = CassandraConfig.newBuilder()
            .setHost(optionsMap.get("host"))
            .setPort(Integer.parseInt(optionsMap.get("port")))
            .build();
        return servingStoreBuilder.setCassandraConfig(cassandraConfig).build();
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported store type %s provided", type));
    }
  }
}
