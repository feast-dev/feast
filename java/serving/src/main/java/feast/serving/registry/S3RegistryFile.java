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
package feast.serving.registry;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import feast.proto.core.RegistryProto;
import java.io.IOException;
import java.util.Optional;

public class S3RegistryFile implements RegistryFile {
  private final AmazonS3 s3Client;
  private S3Object s3Object;

  public S3RegistryFile(AmazonS3 s3Client, String url) {
    this.s3Client = s3Client;

    String[] split = url.replace("s3://", "").split("/");
    String objectPath = String.join("/", java.util.Arrays.copyOfRange(split, 1, split.length));
    this.s3Object = this.s3Client.getObject(split[0], objectPath);
  }

  @Override
  public RegistryProto.Registry getContent() {
    S3ObjectInputStream is = this.s3Object.getObjectContent();

    try {
      return RegistryProto.Registry.parseFrom(is);
    } catch (IOException e) {
      throw new RuntimeException(
          String.format(
              "Couldn't read remote registry file: %s. Error: %s",
              String.format("s3://%s/%s", this.s3Object.getBucketName(), this.s3Object.getKey()),
              e.getMessage()));
    } finally {
      try {
        is.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public Optional<RegistryProto.Registry> getContentIfModified() {
    GetObjectRequest request =
        new GetObjectRequest(this.s3Object.getBucketName(), this.s3Object.getKey())
            .withNonmatchingETagConstraint(this.s3Object.getObjectMetadata().getETag());

    S3Object updatedS3Object;
    try {
      updatedS3Object = this.s3Client.getObject(request);
    } catch (AmazonServiceException e) {
      e.printStackTrace();
      return Optional.empty();
    }

    if (updatedS3Object == null) {
      return Optional.empty();
    }

    this.s3Object = updatedS3Object;
    return Optional.of(this.getContent());
  }
}
