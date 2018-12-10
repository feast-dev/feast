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

package feast.ingestion.transform;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.types.FeatureRowProto.FeatureRow;
import lombok.Builder;
import lombok.NonNull;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;

public class FeatureRowJsonTextIO {

  public static Read read(ImportSpec importSpec) {
    return Read.builder().importSpec(importSpec).build();
  }

  /**
   * Transform for processing JSON text jsonfiles and that contain JSON serialised FeatureRow messages,
   * one per line.
   *
   * <p>This transform asserts that the import spec is for only one entity, as all columns must have
   * the same entity.
   *
   * <p>The output is a PCollection of {@link feast.types.FeatureRowProto.FeatureRow FeatureRows}.
   */
  @Builder
  public static class Read extends FeatureIO.Read {

    @NonNull private final ImportSpec importSpec;

    @Override
    public PCollection<FeatureRow> expand(PInput input) {
      String path = importSpec.getOptionsOrDefault("path", null);
      Preconditions.checkNotNull(path, "Path must be set in for file import");
      PCollection<String> jsonLines = input.getPipeline().apply(TextIO.read().from(path));

      return jsonLines.apply(
          ParDo.of(
              new DoFn<String, FeatureRow>() {
                @ProcessElement
                public void processElement(ProcessContext context) {
                  String line = context.element();
                  FeatureRow.Builder builder = FeatureRow.newBuilder();
                  try {
                    // TODO use proto registry so that it can read Any feature values, not just
                    // primitives.
                    JsonFormat.parser().merge(line, builder);
                    context.output(builder.build());
                  } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                  }
                }
              }));
    }
  }
}
