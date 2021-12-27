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

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import feast.proto.core.RegistryProto;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.junit.jupiter.Container;

public class ServingRedisS3RegistryIT extends ServingBase {
  @Container private static final S3MockContainer s3Mock = new S3MockContainer("2.2.3");

  private static AmazonS3 createClient() {
    return AmazonS3ClientBuilder.standard()
        .withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(
                String.format("http://localhost:%d", s3Mock.getHttpServerPort()), "us-east-1"))
        .enablePathStyleAccess()
        .build();
  }

  private static void putToStorage(RegistryProto.Registry proto) {
    byte[] bytes = proto.toByteArray();
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(bytes.length);
    metadata.setContentType("application/protobuf");

    AmazonS3 s3Client = createClient();
    s3Client.putObject("test-bucket", "registry.db", new ByteArrayInputStream(bytes), metadata);
  }

  @BeforeAll
  static void setUp() throws IOException {
    AmazonS3 s3Client = createClient();
    s3Client.createBucket("test-bucket");

    putToStorage(registryProto);
  }

  @Override
  void updateRegistryFile(RegistryProto.Registry registry) {
    putToStorage(registry);
  }
}
