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

import com.google.cloud.storage.*;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.proto.core.RegistryProto;
import java.util.Optional;

public class GSRegistryFile implements RegistryFile {
  private Blob blob;

  public GSRegistryFile(Storage storage, String url) {
    blob = storage.get(BlobId.fromGsUtilUri(url));
    if (blob == null) {
      throw new RuntimeException(String.format("Registry file %s was not found", url));
    }
  }

  public RegistryProto.Registry getContent() {
    try {
      return RegistryProto.Registry.parseFrom(blob.getContent());
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(
          String.format(
              "Couldn't read remote registry: %s. Error: %s",
              blob.getBlobId().toGsUtilUri(), e.getMessage()));
    }
  }

  public Optional<RegistryProto.Registry> getContentIfModified() {
    try {
      this.blob = blob.reload(Blob.BlobSourceOption.generationNotMatch());
    } catch (StorageException e) {
      if (e.getCode() == 304) {
        // Content not modified
        return Optional.empty();
      } else {
        throw new RuntimeException(
            String.format(
                "Couldn't read remote registry: %s. Error: %s",
                blob.getBlobId().toGsUtilUri(), e.getMessage()));
      }
    }

    return Optional.of(this.getContent());
  }
}
