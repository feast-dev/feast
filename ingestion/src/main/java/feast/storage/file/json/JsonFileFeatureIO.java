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
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import feast.ingestion.transform.FeatureIO;
import feast.storage.file.FileStoreOptions;
import feast.storage.file.TextFileFeatureIO;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import feast.types.FeatureRowProto.FeatureRow;

public class JsonFileFeatureIO {

  public static Write writeRowExtended(FileStoreOptions options) {
    return new Write(options, (rowExtended) -> rowExtended);
  }

  public static Write writeRow(FileStoreOptions options) {
    return new Write(options, (rowExtended) -> rowExtended.getRow());
  }

  /**
   * Transform for processing JSON text jsonfiles and that contain JSON serialised FeatureRow
   * messages, one per line.
   *
   * <p>This transform asserts that the import spec is for only one entity, as all columns must have
   * the same entity.
   *
   * <p>The output is a PCollection of {@link feast.types.FeatureRowProto.FeatureRow
   * FeatureRows}.
   */
  @Builder
  public static class Read extends FeatureIO.Read {

    @NonNull private final JsonFileSourceOptions options;

    @Override
    public PCollection<FeatureRow> expand(PInput input) {
      PCollection<String> jsonLines = input.getPipeline().apply(TextIO.read().from(options.path));

      return jsonLines.apply(
          ParDo.of(
              new DoFn<String, FeatureRow>() {
                @ProcessElement
                public void processElement(ProcessContext context) {
                  String line = context.element();
                  FeatureRow.Builder builder = FeatureRow.newBuilder();
                  try {
                    JsonFormat.parser().merge(line, builder);
                    context.output(builder.build());
                  } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                  }
                }
              }));
    }
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
