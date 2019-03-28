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

package feast;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.gson.Gson;
import java.io.IOException;
import java.io.OutputStream;

public class TestHelper {
  public static void writeYaml(Object model, OutputStream outputStream) throws IOException {
    // convoluted way to make an import spec, converted it to json with gson, then to object with
    // jackson, then to yaml.
    String json = new Gson().toJson(model);
    Object object = new ObjectMapper(new JsonFactory()).reader().readTree(json);
    new ObjectMapper(new YAMLFactory()).writer().writeValue(outputStream, object);
  }
}
