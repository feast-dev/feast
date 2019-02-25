/*
 * Copyright 2019 The Feast Authors
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

package feast.store;

import lombok.AllArgsConstructor;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;

public class TextFileDynamicIO {

  private TextFileDynamicIO() {
  }

  @AllArgsConstructor
  public static class Write extends PTransform<PCollection<KV<String, String>>, PDone> {

    private final FileStoreOptions options;
    private final String suffix;

    /**
     * Writes to different file sinks based on a
     */
    @Override
    public PDone expand(PCollection<KV<String, String>> input) {
      final String folderName = options.jobName != null ? options.jobName : "unknown-jobs";
      FileIO.Write<String, KV<String, String>> write =
          FileIO.<String, KV<String, String>>writeDynamic()
              .by(KV::getKey)
              .withDestinationCoder(StringUtf8Coder.of())
              .withNaming(
                  Contextful.fn(
                      (key) -> FileIO.Write.defaultNaming(folderName + "/" + key + "/part-", suffix)))
              .via(Contextful.fn(KV::getValue), Contextful.fn((entityName) -> TextIO.sink()))
              .to(options.path);

      if (input.isBounded().equals(IsBounded.UNBOUNDED)) {
        Window<KV<String, String>> minuteWindow =
            Window.<KV<String, String>>into(FixedWindows.of(options.getWindowDuration()))
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
