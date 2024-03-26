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

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobServiceClient;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.proto.core.RegistryProto;
import java.util.Objects;
import java.util.Optional;

public class AzureRegistryFile implements RegistryFile {
  private final BlobClient blobClient;
  private String lastKnownETag;

  public AzureRegistryFile(BlobServiceClient blobServiceClient, String url) {
    String[] split = url.replace("az://", "").split("/");
    String objectPath = String.join("/", java.util.Arrays.copyOfRange(split, 1, split.length));
    this.blobClient = blobServiceClient.getBlobContainerClient(split[0]).getBlobClient(objectPath);
  }

  @Override
  public RegistryProto.Registry getContent() {
    try {
      return RegistryProto.Registry.parseFrom(blobClient.downloadContent().toBytes());
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(
          String.format(
              "Couldn't read remote registry: %s. Error: %s",
              blobClient.getBlobUrl(), e.getMessage()));
    }
  }

  @Override
  public Optional<RegistryProto.Registry> getContentIfModified() {
    String eTag = blobClient.getProperties().getETag();
    if (Objects.equals(eTag, this.lastKnownETag)) {
      return Optional.empty();
    } else this.lastKnownETag = eTag;

    return Optional.of(getContent());
  }
}
