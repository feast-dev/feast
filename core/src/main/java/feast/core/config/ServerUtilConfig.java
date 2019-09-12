/*
 * Copyright 2018 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package feast.core.config;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import feast.core.config.StorageConfig.StorageSpecs;
import feast.core.dao.EntityInfoRepository;
import feast.core.dao.FeatureGroupInfoRepository;
import feast.core.dao.FeatureInfoRepository;
import feast.core.storage.BigQueryStorageManager;
import feast.core.storage.SchemaManager;
import feast.core.storage.ViewTemplater;
import feast.core.validators.SpecValidator;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

/**
 * Configuration providing utility objects for the core application.
 */
@Configuration
public class ServerUtilConfig {


  @Autowired
  private StorageSpecs storageSpecs;

  /**
   * Get a warehouse view templater.
   *
   * @return ViewTemplater
   */
  @Bean
  public ViewTemplater viewTemplater(
    @Value("${feast.store.warehouse.type}") String warehouseType) throws IOException {
    String templateSource;
    String templateName;
    switch (warehouseType) {
      case BigQueryStorageManager.TYPE:
        templateSource = "templates/bq_view.tmpl";
        templateName = "bqViewTemplate";
        break;
      default:
        throw new IllegalArgumentException(String.format("Unknown warehouse type: %s", warehouseType));
    }
    Resource resource = new ClassPathResource(templateSource);
    InputStream resourceInputStream = resource.getInputStream();
    String tmpl = CharStreams.toString(new InputStreamReader(resourceInputStream, Charsets.UTF_8));
    return new ViewTemplater(tmpl, templateName);
  }


  /**
   * Get the storage schema manager.
   *
   * @return SchemaManager
   */
  @Bean
  public SchemaManager schemaManager(ViewTemplater viewTemplater) {
    return new SchemaManager(viewTemplater, storageSpecs);

  }

  /**
   * Get a spec validator.
   *
   * @return SpecValidator
   */
  @Bean
  public SpecValidator specValidator(
      EntityInfoRepository entityInfoRepository,
      FeatureGroupInfoRepository featureGroupInfoRepository,
      FeatureInfoRepository featureInfoRepository) {
    SpecValidator specValidator =
        new SpecValidator(
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    return specValidator;
  }
}
