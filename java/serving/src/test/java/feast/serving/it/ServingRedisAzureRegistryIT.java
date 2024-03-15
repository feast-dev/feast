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
package feast.serving.it;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import feast.proto.core.RegistryProto;
import feast.serving.service.config.ApplicationProperties;
import java.io.ByteArrayInputStream;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;

public class ServingRedisAzureRegistryIT extends ServingBaseTests {
  private static final String TEST_ACCOUNT_NAME = "devstoreaccount1";
  private static final String TEST_ACCOUNT_KEY =
      "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
  private static final int BLOB_STORAGE_PORT = 10000;
  private static final String TEST_CONTAINER = "test-container";
  private static final StorageSharedKeyCredential CREDENTIAL =
      new StorageSharedKeyCredential(TEST_ACCOUNT_NAME, TEST_ACCOUNT_KEY);

  @Container
  static final GenericContainer<?> azureBlobMock =
      new GenericContainer<>("mcr.microsoft.com/azure-storage/azurite:latest")
          .waitingFor(Wait.forLogMessage("Azurite Blob service successfully listens on.*", 1))
          .withExposedPorts(BLOB_STORAGE_PORT)
          .withCommand("azurite-blob", "--blobHost", "0.0.0.0");

  private static BlobServiceClient createClient() {
    return new BlobServiceClientBuilder()
        .endpoint(
            String.format(
                "http://localhost:%d/%s",
                azureBlobMock.getMappedPort(BLOB_STORAGE_PORT), TEST_ACCOUNT_NAME))
        .credential(CREDENTIAL)
        .buildClient();
  }

  private static void putToStorage(RegistryProto.Registry registry) {
    BlobServiceClient client = createClient();
    BlobClient blobClient =
        client.getBlobContainerClient(TEST_CONTAINER).getBlobClient("registry.db");

    blobClient.upload(new ByteArrayInputStream(registry.toByteArray()));
  }

  @BeforeAll
  static void setUp() {
    BlobServiceClient client = createClient();
    client.createBlobContainer(TEST_CONTAINER);

    putToStorage(registryProto);
  }

  @Override
  ApplicationProperties.FeastProperties createFeastProperties() {
    final ApplicationProperties.FeastProperties feastProperties =
        TestUtils.createBasicFeastProperties(
            environment.getServiceHost("redis", 6379), environment.getServicePort("redis", 6379));
    feastProperties.setRegistry(String.format("az://%s/registry.db", TEST_CONTAINER));

    return feastProperties;
  }

  @Override
  void updateRegistryFile(RegistryProto.Registry registry) {
    putToStorage(registry);
  }

  @Override
  AbstractModule registryConfig() {
    return new AbstractModule() {
      @Provides
      public BlobServiceClient awsStorage() {
        return new BlobServiceClientBuilder()
            .endpoint(
                String.format(
                    "http://localhost:%d/%s",
                    azureBlobMock.getMappedPort(BLOB_STORAGE_PORT), TEST_ACCOUNT_NAME))
            .credential(CREDENTIAL)
            .buildClient();
      }
    };
  }
}
