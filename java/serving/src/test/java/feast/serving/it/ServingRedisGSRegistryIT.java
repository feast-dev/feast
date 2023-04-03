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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import com.google.cloud.storage.*;
import com.google.cloud.storage.testing.RemoteStorageHelper;
import feast.proto.core.RegistryProto;
import feast.serving.service.config.ApplicationProperties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class ServingRedisGSRegistryIT extends ServingBaseTests {
  static Storage storage =
      RemoteStorageHelper.create()
          .getOptions()
          .toBuilder()
          .setProjectId(System.getProperty("GCP_PROJECT", "kf-feast"))
          .build()
          .getService();

  static final String bucket = RemoteStorageHelper.generateBucketName();

  static void putToStorage(BlobId blobId, RegistryProto.Registry registry) {
    storage.create(BlobInfo.newBuilder(blobId).build(), registry.toByteArray());

    assertArrayEquals(storage.get(blobId).getContent(), registry.toByteArray());
  }

  static BlobId blobId;

  @BeforeAll
  static void setUp() {
    storage.create(BucketInfo.of(bucket));
    blobId = BlobId.of(bucket, "registry.db");

    putToStorage(blobId, registryProto);
  }

  @AfterAll
  static void tearDown() throws ExecutionException, InterruptedException {
    RemoteStorageHelper.forceDelete(storage, bucket, 5, TimeUnit.SECONDS);
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
    putToStorage(blobId, registry);
  }
}
