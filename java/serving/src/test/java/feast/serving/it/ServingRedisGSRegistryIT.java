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

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.*;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import feast.proto.core.RegistryProto;
import feast.serving.service.config.ApplicationProperties;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;

public class ServingRedisGSRegistryIT extends ServingBaseTests {

  private static final String TEST_PROJECT = "test-project";
  private static final String TEST_BUCKET = "test-bucket";
  private static final BlobId blobId = BlobId.of(TEST_BUCKET, "registry.db");;
  private static final int GCS_PORT = 4443;

  @Container
  static final GenericContainer<?> gcsMock =
      new GenericContainer<>("fsouza/fake-gcs-server")
          .withExposedPorts(GCS_PORT)
          .withCreateContainerCmdModifier(
              cmd -> cmd.withEntrypoint("/bin/fake-gcs-server", "-scheme", "http"));

  public static final AccessToken credential = new AccessToken("test-token", null);

  static void putToStorage(RegistryProto.Registry registry) {
    Storage gcsClient = createClient();

    gcsClient.create(BlobInfo.newBuilder(blobId).build(), registry.toByteArray());
  }

  @BeforeAll
  static void setUp() {
    Storage gcsClient = createClient();
    gcsClient.create(BucketInfo.of(TEST_BUCKET));

    putToStorage(registryProto);
  }

  private static Storage createClient() {
    return StorageOptions.newBuilder()
        .setProjectId(TEST_PROJECT)
        .setCredentials(ServiceAccountCredentials.create(credential))
        .setHost(String.format("http://%s:%d", gcsMock.getHost(), gcsMock.getMappedPort(GCS_PORT)))
        .build()
        .getService();
  }

  @Override
  ApplicationProperties.FeastProperties createFeastProperties() {
    final ApplicationProperties.FeastProperties feastProperties =
        TestUtils.createBasicFeastProperties(
            environment.getServiceHost("redis", 6379), environment.getServicePort("redis", 6379));
    feastProperties.setRegistry(blobId.toGsUtilUri());

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
      Storage googleStorage(ApplicationProperties applicationProperties) {
        return StorageOptions.newBuilder()
            .setProjectId(TEST_PROJECT)
            .setCredentials(ServiceAccountCredentials.create(credential))
            .setHost(
                String.format("http://%s:%d", gcsMock.getHost(), gcsMock.getMappedPort(GCS_PORT)))
            .build()
            .getService();
      }
    };
  }
}
