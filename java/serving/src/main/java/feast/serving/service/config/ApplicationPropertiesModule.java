/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2021 The Feast Authors
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
 */
package feast.serving.service.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.io.Resources;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class ApplicationPropertiesModule extends AbstractModule {
  private final String[] args;

  public ApplicationPropertiesModule(String[] args) {
    this.args = args;
  }

  @Provides
  @Singleton
  public ApplicationProperties provideApplicationProperties() throws IOException {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    mapper.findAndRegisterModules();
    mapper.setDefaultMergeable(Boolean.TRUE);
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    ApplicationProperties properties = new ApplicationProperties();
    ObjectReader objectReader = mapper.readerForUpdating(properties);

    String[] filePaths = this.args[0].split(",");
    for (String filePath : filePaths) {
      objectReader.readValue(readPropertiesFile(filePath));
    }

    return properties;
  }

  /**
   * Read file path in spring compatible format, eg classpath:/application.yml or
   * file:/path/application.yml
   */
  private byte[] readPropertiesFile(String filePath) throws IOException {
    if (filePath.startsWith("classpath:")) {
      filePath = filePath.substring("classpath:".length());
      if (filePath.startsWith("/")) {
        filePath = filePath.substring(1);
      }

      return Resources.toByteArray(Resources.getResource(filePath));
    }

    if (filePath.startsWith("file")) {
      filePath = filePath.substring("file:".length());
    }

    return Files.readAllBytes(Path.of(filePath));
  }
}
