package feast.core.config;

import feast.core.config.StorageConfig.StorageSpecs;
import feast.core.storage.BigQueryStorageManager;
import feast.specs.StorageSpecProto.StorageSpec;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration specific for warehouse storage
 */
@Configuration
@AllArgsConstructor
public class WarehouseConfig {

  @Bean
  public WarehouseSpec getMissingWarehouseSpec() {
    return WarehouseSpec.builder()
      .trainingTemplateResource("templates/bq_training.tmpl")
      .viewTemplateResource("templates/bq_view.tmpl")
      .build();
  }

  @Bean
  @ConditionalOnProperty(
    name = "feast.store.warehouse.type",
    havingValue = "bigquery")
  public WarehouseSpec getBigQueryWarehouseSpec(StorageSpecs storageSpecs) {
    StorageSpec warehouseSpec = storageSpecs.getWarehouseStorageSpec();
    String projectId = warehouseSpec.getOptionsOrDefault(BigQueryStorageManager.OPT_BIGQUERY_PROJECT, null);
    String dataset = warehouseSpec.getOptionsOrDefault(BigQueryStorageManager.OPT_BIGQUERY_DATASET, null);
    String linkUrlBase = String.format("https://bigquery.cloud.google.com/table/%s:%s.", projectId, dataset);
    String tableIdBase = String.format("%s.%s.", projectId, dataset);
    return WarehouseSpec.builder()
      .trainingTemplateResource("templates/bq_training.tmpl")
      .viewTemplateResource("templates/bq_view.tmpl")
      .linkUrlFormat(linkUrlBase + "%s_view")
      .tableIdFormat(tableIdBase + "%s")
      .build();
  }

  @Builder
  public static class WarehouseSpec {
    @Getter private String viewTemplateResource;
    @Getter private String trainingTemplateResource;
    private String linkUrlFormat;
    private String tableIdFormat;

    public String createLinkUrl(String entityName) {
      return String.format(linkUrlFormat, entityName);
    }

    public String createTableId(String entityName) {
      return String.format(tableIdFormat, entityName);
    }
  }

}
