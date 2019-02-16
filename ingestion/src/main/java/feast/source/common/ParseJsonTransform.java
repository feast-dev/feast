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

package feast.source.common;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.reflect.TypeToken;
import feast.ingestion.model.Values;
import feast.types.ValueProto.Value;
import java.lang.reflect.Type;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class ParseJsonTransform extends
    PTransform<PCollection<String>, PCollection<Map<String, Value>>> {

  @Override
  public PCollection<Map<String, Value>> expand(PCollection<String> input) {
    return input.apply(ParDo.of(new ParseJsonDoFn()));
  }


  public static class ParseJsonDoFn extends DoFn<String, Map<String, Value>> {

    private transient Gson gson;
    private transient Type valueMapType;

    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output(parseJson(context.element()));
    }

    Map<String, Value> parseJson(String json) {
      if (gson == null) {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(Value.class, new ValueDeserializer());
        gson = gsonBuilder.create();
        valueMapType = new TypeToken<Map<String, Value>>() {
        }.getType();
      }
      return gson.fromJson(json, valueMapType);
    }
  }

  private static class ValueDeserializer implements JsonDeserializer<Value> {

    @Override
    public Value deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
        throws JsonParseException {
      if (json.isJsonNull()) {
        return Value.newBuilder().build(); // return UNKNOWN value.
      } else if (json.isJsonPrimitive()) {
        JsonPrimitive primitive = json.getAsJsonPrimitive();
        if (primitive.isBoolean()) {
          return Values.ofBool(primitive.getAsBoolean());
        } else if (primitive.isString()) {
          return Values.ofString(primitive.getAsString());
        } else {
          // Find out if it is an int type
          Double doubleVal = primitive.getAsDouble();
          Long int64Val = primitive.getAsLong();

          if (Double.compare(int64Val.doubleValue(), doubleVal) != 0.0) {
            return Values.ofDouble(doubleVal);
          } else {
            return Values.ofInt64(int64Val);
          }
        }
      } else if (json.isJsonArray() || json.isJsonObject()) {
        return Values.ofString(json.toString());
      } else {
        throw new JsonParseException("Unknown json element type ");
      }
    }
  }
}