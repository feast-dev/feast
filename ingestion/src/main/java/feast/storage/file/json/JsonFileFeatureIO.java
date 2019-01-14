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

package feast.storage.file.json;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import feast.ingestion.transform.FeatureIO;
import feast.storage.file.FileStoreOptions;
import feast.storage.file.TextFileFeatureIO;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import lombok.AllArgsConstructor;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

public class JsonFileFeatureIO {

  public static Write writeRowExtended(FileStoreOptions options) {
    return new Write(options, (rowExtended) -> rowExtended);
  }

  public static Write writeRow(FileStoreOptions options) {
    return new Write(options, (rowExtended) -> rowExtended.getRow());
  }

  @AllArgsConstructor
  public static class Write extends FeatureIO.Write {

    private FileStoreOptions options;
    private SerializableFunction<FeatureRowExtended, Message> messageFunction;

    @Override
    public PDone expand(PCollection<FeatureRowExtended> input) {
      return input.apply(
          "Write Text Files",
          new TextFileFeatureIO.Write(
              options,
              (rowExtended) -> {
                try {
                  return JsonFormat.printer()
                      .omittingInsignificantWhitespace()
                      .print(messageFunction.apply(rowExtended));
                } catch (InvalidProtocolBufferException e) {
                  throw new RuntimeException(e);
                }
              },
              ".json"));
    }
  }
}
