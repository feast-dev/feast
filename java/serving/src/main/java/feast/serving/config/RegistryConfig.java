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
package feast.serving.config;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import feast.serving.registry.*;
import java.net.URI;
import java.util.Optional;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

@Configuration
public class RegistryConfig {
  @Bean
  @Lazy
  Storage googleStorage(FeastProperties feastProperties) {
    return StorageOptions.newBuilder()
        .setProjectId(feastProperties.getGcpProject())
        .build()
        .getService();
  }

  @Bean
  @Lazy
  AmazonS3 awsStorage(FeastProperties feastProperties) {
    return AmazonS3ClientBuilder.standard().withRegion(feastProperties.getAwsRegion()).build();
  }

  @Bean
  RegistryFile registryFile(FeastProperties feastProperties, ApplicationContext context) {

    String registryPath = feastProperties.getRegistry();
    Optional<String> scheme = Optional.ofNullable(URI.create(registryPath).getScheme());

    switch (scheme.orElseGet(() -> "")) {
      case "gs":
        return new GSRegistryFile(context.getBean(Storage.class), registryPath);
      case "s3":
        return new S3RegistryFile(context.getBean(AmazonS3.class), registryPath);
      case "":
      case "file":
        return new LocalRegistryFile(registryPath);
      default:
        throw new RuntimeException("Registry storage %s is unsupported");
    }
  }

  @Bean
  RegistryRepository registryRepository(
      RegistryFile registryFile, FeastProperties feastProperties) {
    return new RegistryRepository(registryFile, feastProperties.getRegistryRefreshInterval());
  }
}
