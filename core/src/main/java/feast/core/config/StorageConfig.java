package feast.core.config;

import static feast.core.util.TypeConversion.convertJsonStringToMap;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import feast.core.validators.SpecValidator;
import feast.specs.StorageSpecProto.StorageSpec;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@AllArgsConstructor
@NoArgsConstructor
public class StorageConfig {

  public static final String DEFAULT_SERVING_ID = "SERVING";
  public static final String DEFAULT_WAREHOUSE_ID = "WAREHOUSE";
  public static final String DEFAULT_ERRORS_ID = "ERRORS";

  @Autowired
  private SpecValidator validator;

  private StorageSpec buildStorageSpec(
      String id,
      String type,
      String options) {
    options = Strings.isNullOrEmpty(options) ? "{}" : options;
    Map<String, String> optionsMap = convertJsonStringToMap(options);
    if (Strings.isNullOrEmpty(type)) {
      return null;
    }
    StorageSpec storageSpec = StorageSpec.newBuilder()
        .setId(id)
        .setType(type)
        .putAllOptions(optionsMap)
        .build();
    switch (id) {
      case DEFAULT_SERVING_ID:
        validator.validateServingStorageSpec(storageSpec);
        break;
      case DEFAULT_WAREHOUSE_ID:
        validator.validateWarehouseStorageSpec(storageSpec);
        break;
      case DEFAULT_ERRORS_ID:
        validator.validateErrorsStorageSpec(storageSpec);
        break;
    }
    return storageSpec;
  }

  @Bean
  public StorageSpecs getStorageSpecs(
      @Value("${feast.store.serving.type}") String servingType,
      @Value("${feast.store.serving.options}") String servingOptions,
      @Value("${feast.store.warehouse.type}") String warehouseType,
      @Value("${feast.store.warehouse.options}") String warehouseOptions,
      @Value("${feast.store.errors.type}") String errorsType,
      @Value("${feast.store.errors.options}") String errorsOptions) {
    StorageSpecs storageSpecs = StorageSpecs.builder()
        .servingStorageSpec(buildStorageSpec(DEFAULT_SERVING_ID, servingType, servingOptions))
        .warehouseStorageSpec(
            buildStorageSpec(DEFAULT_WAREHOUSE_ID, warehouseType, warehouseOptions))
        .errorsStorageSpec(buildStorageSpec(DEFAULT_ERRORS_ID, errorsType, errorsOptions))
        .build();
    return storageSpecs;
  }

  @Builder
  @Getter
  public static class StorageSpecs {
    private StorageSpec servingStorageSpec;
    private StorageSpec warehouseStorageSpec;
    private StorageSpec errorsStorageSpec;

    public List<StorageSpec> getSinks() {
      List<StorageSpec> sinks = new ArrayList<>();
      if (exists(servingStorageSpec)) {
        sinks.add(servingStorageSpec);
      }
      if (exists(warehouseStorageSpec)) {
        sinks.add(warehouseStorageSpec);
      }
      return sinks;
    }

    private boolean exists(StorageSpec storageSpec) {
      if (storageSpec == null) {
        return false;
      }
      return storageSpec.getId() != "";
    }
  }
}
