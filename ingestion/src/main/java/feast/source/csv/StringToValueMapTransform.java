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

import com.google.common.base.Strings;
import feast.ingestion.model.Values;
import feast.types.ValueProto.Value;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class StringToValueMapTransform extends
    PTransform<PCollection<Map<String, String>>, PCollection<Map<String, Value>>> {

  public static MapCoder<String, Value> VALUE_MAP_CODER = MapCoder.of(StringUtf8Coder.of(),
      ProtoCoder.of(Value.class));

  @Override
  public PCollection<Map<String, Value>> expand(PCollection<Map<String, String>> input) {
    return input.apply(ParDo.of(new StringToValueMapDoFn()))
        .setCoder(VALUE_MAP_CODER);
  }

  public static class StringToValueMapDoFn extends
      DoFn<Map<String, String>, Map<String, Value>> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      Map<String, Value> row = new HashMap<>();
      for (Entry<String, String> entry : context.element().entrySet()) {
        String stringVal = entry.getValue();
        if (!Strings.isNullOrEmpty(stringVal)) {
          row.put(entry.getKey(), Values.ofString(entry.getValue()));
        }
      }
      context.output(row);
    }
  }
}