package feast.core.config;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.hubspot.jinjava.Jinjava;
import feast.core.config.StorageConfig.StorageSpecs;
import feast.core.dao.FeatureInfoRepository;
import feast.core.training.BigQueryDatasetTemplater;
import feast.core.training.BigQueryTraningDatasetCreator;
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
  public BigQueryDatasetTemplater getBigQueryTrainingDatasetTemplater(
      StorageSpecs storageSpecs, FeatureInfoRepository featureInfoRepository) throws IOException {
    Resource resource = new ClassPathResource("templates/bq_training.tmpl");
    InputStream resourceInputStream = resource.getInputStream();
    String tmpl = CharStreams.toString(new InputStreamReader(resourceInputStream, Charsets.UTF_8));
    return new BigQueryDatasetTemplater(
        new Jinjava(), tmpl, storageSpecs.getWarehouseStorageSpec(), featureInfoRepository);
  }

  @Bean
  public BigQueryTraningDatasetCreator getBigQueryTrainingDatasetCreator(
      BigQueryDatasetTemplater templater,
      @Value("${feast.core.projectId}") String projectId,
      @Value("${feast.core.datasetPrefix}") String datasetPrefix) {
    return new BigQueryTraningDatasetCreator(
        templater, projectId, datasetPrefix, new RandomUuidProvider());
  }
}
