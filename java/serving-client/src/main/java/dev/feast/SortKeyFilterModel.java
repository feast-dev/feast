/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2025 The Feast Authors
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
package dev.feast;

import feast.proto.serving.ServingAPIProto.SortKeyFilter;
import feast.proto.types.ValueProto.Value;

public class SortKeyFilterModel {
  private String sortKeyName;
  private Value equals;
  private RangeQueryModel rangeQueryModel;

  public SortKeyFilterModel(String sortKeyName, RangeQueryModel rangeQueryModel) {
    this.sortKeyName = sortKeyName;
    this.rangeQueryModel = rangeQueryModel;
  }

  public SortKeyFilterModel(String sortKeyName, Object equals) {
    this.sortKeyName = sortKeyName;
    this.equals = RequestUtil.objectToValue(equals);
  }

  public SortKeyFilter toProto() {
    if (equals != null) {
      return SortKeyFilter.newBuilder().setSortKeyName(sortKeyName).setEquals(equals).build();
    }

    return SortKeyFilter.newBuilder()
        .setSortKeyName(sortKeyName)
        .setRange(this.rangeQueryModel.toProto())
        .build();
  }
}
