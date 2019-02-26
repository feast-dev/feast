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

package feast.source.csv;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.NonNull;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

/**
 * Transform for reading text CSV jsonfiles into a map of strings to strings. CSV format is assumed
 * to be RFC4180.
 */
@Builder
public class ParseCsvTransform extends
    PTransform<PCollection<String>, PCollection<Map<String, String>>> {

  @NonNull
  private List<String> header;

  @Override
  public PCollection<Map<String, String>> expand(PCollection<String> input) {
    CSVLineParser csvLineParser = new CSVLineParser(header);
    return input.apply(ParDo.of(new DoFn<String, Map<String, String>>() {
      @ProcessElement
      public void processElement(ProcessContext context) {
        String line = context.element();
        if (line != null && !line.isEmpty()) {
          for (StringMap map : csvLineParser.records(line)) {
            context.output(map);
          }
        }
      }
    })).setCoder(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
  }

  public static class StringMap extends HashMap<String, String> {

    public StringMap() {
      super();
    }

    public StringMap(Map<String, String> map) {
      super();
      this.putAll(map);
    }

    public StringMap thisput(String key, String value) {
      this.put(key, value);
      return this;
    }
  }

  /**
   * This is a helper class to make it easy to use the commons csv parser object for one line
   * without recreating it.
   */
  public static class CSVLineParser implements Serializable {

    private List<String> header;

    public CSVLineParser(List<String> header) {
      this.header = header;
    }

    public List<StringMap> records(String input) {
      try {
        CSVParser parser = CSVFormat.RFC4180
            .withHeader(header.toArray(new String[]{}))
            .parse(new StringReader(input));
        return parser.getRecords().stream()
            .map(record -> new StringMap(record.toMap()))
            .collect(Collectors.toList());
      } catch (IOException e) {
        String message = String.format("OOPS I couldn't parse the csv line! '%s'", input);
        throw new RuntimeException(message);
      }
    }
  }
}
