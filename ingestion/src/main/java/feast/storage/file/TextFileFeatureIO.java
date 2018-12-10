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

package feast.storage.file;

import lombok.AllArgsConstructor;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;
import feast.ingestion.transform.FeatureIO;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;

public class TextFileFeatureIO {
  private TextFileFeatureIO() {}

  @AllArgsConstructor
  public static class Write extends FeatureIO.Write {
    private final FileStoreOptions options;
    private final SerializableFunction<FeatureRowExtended, String> toTextFunction;
    private final String suffix;

    /** Writes to different file sinks based on a */
    @Override
    public PDone expand(PCollection<FeatureRowExtended> input) {
      final String folderName = options.jobName != null ? options.jobName : "unknown-jobs";
      FileIO.Write<String, FeatureRowExtended> write =
          FileIO.<String, FeatureRowExtended>writeDynamic()
              .by((rowExtended) -> rowExtended.getRow().getEntityName())
              .withDestinationCoder(StringUtf8Coder.of())
              .withNaming(
                  Contextful.fn(
                      (entityName) -> FileIO.Write.defaultNaming(folderName + "/" + entityName, suffix)))
              .via(Contextful.fn(toTextFunction), Contextful.fn((entityName) -> TextIO.sink()))
              .to(options.path);

      if (input.isBounded().equals(IsBounded.UNBOUNDED)) {
        Window<FeatureRowExtended> minuteWindow =
            Window.<FeatureRowExtended>into(FixedWindows.of(options.getWindowDuration()))
                .triggering(AfterWatermark.pastEndOfWindow())
                .discardingFiredPanes()
                .withAllowedLateness(Duration.ZERO);
        input = input.apply(minuteWindow);
        write = write.withNumShards(10);
      }
      WriteFilesResult<String> outputFiles = input.apply(write);
      return PDone.in(outputFiles.getPipeline());
    }
  }
}
