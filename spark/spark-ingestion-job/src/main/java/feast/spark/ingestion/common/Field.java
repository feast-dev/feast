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
package feast.spark.ingestion.common;

import feast.proto.types.ValueProto;
import java.io.Serializable;

public class Field implements Serializable {

  private final String name;
  private final ValueProto.ValueType.Enum type;

  public Field(String name, ValueProto.ValueType.Enum type) {
    this.name = name;
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public ValueProto.ValueType.Enum getType() {
    return type;
  }
}
