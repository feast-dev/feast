package feast.core.config;

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
    initStore(storeRepository, "SERVING", servingType, servingOptions);
    initStore(storeRepository, "WAREHOUSE", warehouseType, warehouseOptions);
  }

  private void initStore(StoreRepository storeRepository, String name, String type,
      String options) {
    if (type.equals("")) {
      return;
    }

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
        servingStoreBuilder.setRedisConfig(redisConfig);
        break;
      case BIGQUERY:
        BigQueryConfig bigQueryConfig = BigQueryConfig.newBuilder()
            .setProjectId(optionsMap.get("projectId"))
            .setDatasetId(optionsMap.get("datasetId"))
            .build();
        servingStoreBuilder.setBigqueryConfig(bigQueryConfig);
        break;
      case CASSANDRA:
        CassandraConfig cassandraConfig = CassandraConfig.newBuilder()
            .setHost(optionsMap.get("host"))
            .setPort(Integer.parseInt(optionsMap.get("port")))
            .build();
        servingStoreBuilder.setCassandraConfig(cassandraConfig);
        break;
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported store type %s provided", type));
    }
    storeRepository.save(Store.fromProto(servingStoreBuilder.build()));
  }
}
