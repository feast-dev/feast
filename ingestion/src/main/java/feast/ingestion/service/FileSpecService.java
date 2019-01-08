/*
 * Copyright 2018 The Feast Authors
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
 *
 */

package feast.ingestion.service;

import com.google.common.base.Charsets;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import feast.ingestion.exceptions.SpecNotFound;
import feast.ingestion.util.PathUtil;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.StorageSpecProto.StorageSpec;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class FileSpecService implements SpecService {

  private static final String ENTITY_SPEC = "entity";
  private static final String FEATURE_SPEC = "feature";
  private static final String STORAGE_SPEC = "storage";

  private String basePath;

  private void mergeJsonProto(Path path, Message.Builder builder) {
    try (InputStream inputStream = Files.newInputStream(path)) {
      Reader reader = new InputStreamReader(inputStream);
      JsonFormat.parser().merge(reader, builder);
    } catch (FileNotFoundException e) {
      throw new SpecNotFound("Spec not found at filepath " + path.toString(), e);
    } catch (IOException e) {
      throw new SpecRetrievalException("Error parsing spec at filepath " + path.toString(), e);
    }
  }

  private Path getBasePath() {
    return PathUtil.getPath(basePath);
  }

  private Path getSpecDir(String type) {
    return getBasePath().resolve(type);
  }

  private Path getSpecFile(String type, String id) {
    return getBasePath().resolve(Paths.get(type, String.format("%s.json", id)).toString());
  }

  private <T extends Message> Map<String, T> getSpecs(
      T prototype, String type, Iterable<String> ids) {
    Map<String, T> specs = new HashMap<>();
    for (String id : ids) {
      Message.Builder builder = prototype.toBuilder();
      mergeJsonProto(getSpecFile(type, id), builder);
      //noinspection unchecked
      specs.put(id, (T) builder.build());
    }
    return specs;
  }

  private <T extends Message> Map<String, T> getAllSpecs(
      T prototype, String type, Function<T, String> keyFunc) {
    prototype.toBuilder();
    Map<String, T> specs = new HashMap<>();
    Path dir = getSpecDir(type);
    try {
      List<Path> paths = Files.list(dir).collect(Collectors.toList());
      for (Path path : paths) {
        if (path.toString().endsWith(".json")) {
          Message.Builder builder = prototype.toBuilder();
          mergeJsonProto(path, builder);
          @SuppressWarnings("unchecked")
          T spec = (T) builder.build();
          specs.put(keyFunc.apply(spec), spec);
        }
      }
    } catch (IOException e) {
      throw new SpecRetrievalException("Error listing spec directory " + dir.toString(), e);
    }
    return specs;
  }

  @Override
  public Map<String, EntitySpec> getEntitySpecs(Iterable<String> entityIds) {
    return getSpecs(EntitySpec.getDefaultInstance(), ENTITY_SPEC, entityIds);
  }

  @Override
  public Map<String, FeatureSpec> getFeatureSpecs(Iterable<String> featureIds) {
    return getSpecs(FeatureSpec.getDefaultInstance(), FEATURE_SPEC, featureIds);
  }

  @Override
  public Map<String, StorageSpec> getStorageSpecs(Iterable<String> storageIds) {
    return getSpecs(StorageSpec.getDefaultInstance(), STORAGE_SPEC, storageIds);
  }

  private <T extends Message> void putSpecs(
      String type, Function<T, String> keyFunc, Iterable<T> specs) {
    for (T spec : specs) {
      Path path = getSpecFile(type, keyFunc.apply(spec));
      try {
        String specJson = JsonFormat.printer().print(spec);
        Files.write(path, specJson.getBytes(Charsets.UTF_8));
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(
            "Invalid proto while writing spec to file " + path.toString(), e);
      } catch (IOException e) {
        throw new RuntimeException("Unable to write spec to file " + path.toString(), e);
      }
    }
  }

  // These below functions are useful in tests

  public void putEntitySpecs(Iterable<EntitySpec> entitySpecs) {
    putSpecs(ENTITY_SPEC, EntitySpec::getName, entitySpecs);
  }

  public void putFeatureSpecs(Iterable<FeatureSpec> featureSpecs) {
    putSpecs(FEATURE_SPEC, FeatureSpec::getId, featureSpecs);
  }

  public void putStorageSpecs(Iterable<StorageSpec> storageSpecs) {
    putSpecs(STORAGE_SPEC, StorageSpec::getId, storageSpecs);
  }

  @AllArgsConstructor
  public static class Builder implements SpecService.Builder {

    private String basePath;

    @Override
    public SpecService build() {
      return new FileSpecService(basePath);
    }
  }
}
