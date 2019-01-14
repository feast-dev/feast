package feast.core.config;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.hubspot.jinjava.Jinjava;
import feast.core.dao.FeatureInfoRepository;
import feast.core.training.BigQueryDatasetCreator;
import feast.core.training.BigQueryDatasetTemplater;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Clock;
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
      FeatureInfoRepository featureInfoRepository) throws IOException {
    Resource resource = new ClassPathResource("templates/bq_training.tmpl");
    InputStream resourceInputStream = resource.getInputStream();
    String tmpl = CharStreams.toString(new InputStreamReader(resourceInputStream, Charsets.UTF_8));
    return new BigQueryDatasetTemplater(new Jinjava(), tmpl, featureInfoRepository);
  }

  @Bean
  public BigQueryDatasetCreator getBigQueryTrainingDatasetCreator(
      BigQueryDatasetTemplater templater,
      @Value("${feast.core.projectId}") String projectId,
      @Value("${feast.core.datasetPrefix}") String datasetPrefix) {
    BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(projectId).build().getService();
    Clock clock = Clock.systemUTC();
    return new BigQueryDatasetCreator(templater, bigquery, clock, projectId, datasetPrefix);
  }
}
