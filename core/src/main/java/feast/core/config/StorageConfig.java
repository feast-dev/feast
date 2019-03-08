package feast.core.config;

import static feast.core.util.TypeConversion.convertJsonStringToMap;

import com.google.common.base.Strings;
import feast.specs.StorageSpecProto.StorageSpec;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class StorageConfig {

  public static final String DEFAULT_SERVING_ID = "serving";
  public static final String DEFAULT_WAREHOUSE_ID = "warehouse";
  public static final String DEFAULT_ERRORS_ID = "errors";

  private StorageSpec buildStorageSpec(
      String id,
      String type,
      String options) {
    options = Strings.isNullOrEmpty(options) ? "{}" : options;
    Map<String, String> optionsMap = convertJsonStringToMap(options);
    if (Strings.isNullOrEmpty(type)) {
      return StorageSpec.getDefaultInstance();
    }
    return StorageSpec.newBuilder()
        .setId(id)
        .setType(type)
        .putAllOptions(optionsMap)
        .build();
  }

  @Bean
  public StorageSpecs getStorageSpecs(
      @Value("${feast.store.serving.type}") String servingType,
      @Value("${feast.store.serving.options}") String servingOptions,
      @Value("${feast.store.warehouse.type}") String warehouseType,
      @Value("${feast.store.warehouse.options}") String warehouseOptions,
      @Value("${feast.store.errors.type}") String errorsType,
      @Value("${feast.store.errors.options}") String errorsOptions) {
    return StorageSpecs.builder()
        .servingStorageSpec(buildStorageSpec(DEFAULT_SERVING_ID, servingType, servingOptions))
        .warehouseStorageSpec(
            buildStorageSpec(DEFAULT_WAREHOUSE_ID, warehouseType, warehouseOptions))
        .errorsStorageSpec(buildStorageSpec(DEFAULT_ERRORS_ID, errorsType, errorsOptions))
        .build();
  }

  @Builder
  @Getter
  public static class StorageSpecs {

    private StorageSpec servingStorageSpec;
    private StorageSpec warehouseStorageSpec;
    private StorageSpec errorsStorageSpec;
  }
}
