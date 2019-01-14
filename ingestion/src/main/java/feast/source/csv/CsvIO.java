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
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CsvIO {

  private static final Logger LOGGER = LoggerFactory.getLogger(CsvIO.class);

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


  public static Read read(String inputPath, List<String> header) {
    return Read.builder()
        .inputPath(inputPath)
        .csvLineParser(new CSVLineParser(header))
        .build();
  }

  /**
   * Transform for reading text CSV jsonfiles into a map of strings to strings. CSV format is assumed to
   * be RFC4180.
   */
  @Builder
  public static class Read extends PTransform<PInput, PCollection<StringMap>> {

    @NonNull
    private final CSVLineParser csvLineParser;
    @NonNull
    private String inputPath;

    @Override
    public PCollection<StringMap> expand(PInput input) {
      PCollection<String> text = input.getPipeline().apply(TextIO.read().from(inputPath));
      return text.apply(ParDo.of(new DoFn<String, StringMap>() {
        @ProcessElement
        public void processElement(ProcessContext context) {
          String line = context.element();
          if (line != null && !line.isEmpty()) {
            for (StringMap map : csvLineParser.records(line)) {
              context.output(map);
            }
          }
        }
      })).setCoder(SerializableCoder.of(StringMap.class));
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
