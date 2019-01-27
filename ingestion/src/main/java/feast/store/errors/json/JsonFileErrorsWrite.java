/*
 * Copyright 2019 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package feast.store.errors.json;

import static org.apache.beam.sdk.values.TypeDescriptors.kvs;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import feast.store.FeatureStoreWrite;
import feast.store.FileStoreOptions;
import feast.store.TextFileDynamicIO;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import lombok.AllArgsConstructor;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

@AllArgsConstructor
public class JsonFileErrorsWrite extends FeatureStoreWrite {

  private FileStoreOptions options;

  @Override
  public PDone expand(PCollection<FeatureRowExtended> input) {
    return input.apply("Map to strings", MapElements.into(kvs(strings(), strings())).via(
        (rowExtended) -> {
          try {
            return KV.of(
                rowExtended.getRow().getEntityName(),
                JsonFormat.printer().omittingInsignificantWhitespace()
                    .print(rowExtended)
            );
          } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
          }
        }
    )).apply("Write Error Json Files", new TextFileDynamicIO.Write(options, ".json"));
  }
}
