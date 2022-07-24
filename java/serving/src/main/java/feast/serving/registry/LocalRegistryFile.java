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

import com.google.protobuf.InvalidProtocolBufferException;
import feast.proto.core.RegistryProto;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;

public class LocalRegistryFile implements RegistryFile {
  private final RegistryProto.Registry cachedRegistry;

  public LocalRegistryFile(String path) {
    try {
      cachedRegistry = RegistryProto.Registry.parseFrom(Files.readAllBytes(Paths.get(path)));
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(
          String.format(
              "Couldn't read local registry: %s. Protobuf is invalid: %s", path, e.getMessage()));
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Couldn't read local registry file: %s. Error: %s", path, e.getMessage()));
    }
  }

  @Override
  public RegistryProto.Registry getContent() {
    return this.cachedRegistry;
  }

  @Override
  public Optional<RegistryProto.Registry> getContentIfModified() {
    return Optional.empty();
  }
}
