package feast.core.config;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.hubspot.jinjava.Jinjava;
import feast.core.config.StorageConfig.StorageSpecs;
import feast.core.dao.FeatureInfoRepository;
import feast.core.storage.BigQueryStorageManager;
import feast.core.training.DatasetTemplater;
import feast.core.training.BigQueryTrainingDatasetCreator;
import feast.core.training.TrainingDatasetCreator;
import feast.core.util.RandomUuidProvider;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

/** Configuration related to training API */
@Configuration
public class TrainingConfig {

  @Bean
  public TrainingDatasetCreator getTrainingDatasetCreator(
      StorageSpecs storageSpecs,
      FeatureInfoRepository featureInfoRepository,
      @Value("${feast.core.projectId}") String projectId,
      @Value("${feast.core.datasetPrefix}") String datasetPrefix,
      @Value("${feast.store.warehouse.type}") String warehouseType) throws IOException {
    switch (warehouseType) {
      case BigQueryStorageManager.TYPE:
        Resource resource = new ClassPathResource("templates/bq_training.tmpl");
        InputStream resourceInputStream = resource.getInputStream();
        String tmpl = CharStreams.toString(new InputStreamReader(resourceInputStream, Charsets.UTF_8));
        return new BigQueryTrainingDatasetCreator(
          new DatasetTemplater(
            new Jinjava(), tmpl, storageSpecs.getWarehouseStorageSpec(), featureInfoRepository),
          projectId, datasetPrefix, new RandomUuidProvider());
      default:
        throw new UnsupportedOperationException(String.format("Unknown warehouse type: %s", warehouseType));
    }
  }
}
