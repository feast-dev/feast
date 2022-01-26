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
import com.google.inject.AbstractModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import feast.serving.registry.*;
import java.net.URI;
import java.util.Optional;

public class RegistryConfig extends AbstractModule {
  @Provides
  Storage googleStorage(ApplicationProperties applicationProperties) {
    return StorageOptions.newBuilder()
        .setProjectId(applicationProperties.getFeast().getGcpProject())
        .build()
        .getService();
  }

  @Provides
  public AmazonS3 awsStorage(ApplicationProperties applicationProperties) {
    return AmazonS3ClientBuilder.standard()
        .withRegion(applicationProperties.getFeast().getAwsRegion())
        .build();
  }

  @Provides
  RegistryFile registryFile(
      ApplicationProperties applicationProperties,
      Provider<Storage> storageProvider,
      Provider<AmazonS3> amazonS3Provider) {

    String registryPath = applicationProperties.getFeast().getRegistry();
    Optional<String> scheme = Optional.ofNullable(URI.create(registryPath).getScheme());

    switch (scheme.orElse("")) {
      case "gs":
        return new GSRegistryFile(storageProvider.get(), registryPath);
      case "s3":
        return new S3RegistryFile(amazonS3Provider.get(), registryPath);
      case "":
      case "file":
        return new LocalRegistryFile(registryPath);
      default:
        throw new RuntimeException("Registry storage %s is unsupported");
    }
  }

  @Provides
  RegistryRepository registryRepository(
      RegistryFile registryFile, ApplicationProperties applicationProperties) {
    return new RegistryRepository(
        registryFile, applicationProperties.getFeast().getRegistryRefreshInterval());
  }
}
