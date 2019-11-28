/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
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
package feast.ingestion.values;

import feast.types.ValueProto.ValueType;
import java.io.Serializable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

/**
 * Field class represents {@link feast.types.FieldProto.Field} but without value.
 *
 * <p>The use for this class is mainly for validating the Fields in FeatureRow.
 */
@DefaultCoder(AvroCoder.class)
public class Field implements Serializable {
  private final String name;
  private final ValueType.Enum type;

  public Field(String name, ValueType.Enum type) {
    this.name = name;
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public ValueType.Enum getType() {
    return type;
  }
}
