/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
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
 */
package feast.core.job.option;

import feast.core.util.TypeConversion;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class RunnerConfig {
  public String[] toArgs() throws IllegalAccessException {
    List<String> args = new ArrayList<>();
    for (Field field : this.getClass().getFields()) {
      if (field.get(this) == null) {
        continue;
      }
      Class<?> type = field.getType();
      if (Map.class.equals(type)) {
        String jsonString =
            TypeConversion.convertMapToJsonString((Map<String, String>) field.get(this));
        args.add(String.format("--%s=%s", field.getName(), jsonString));
        continue;
      }

      if (String.class.equals(type)) {
        String val = (String) field.get(this);
        if (!val.equals("")) {
          args.add(String.format("--%s=%s", field.getName(), val));
        }
        continue;
      }

      if (Integer.class.equals(type)) {
        Integer val = (Integer) field.get(this);
        if (val != 0) {
          args.add(String.format("--%s=%d", field.getName(), val));
        }
        continue;
      }

      args.add(String.format("--%s=%s", field.getName(), field.get(this)));
    }
    return args.toArray(String[]::new);
  }
}
