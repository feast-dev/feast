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

import feast.proto.serving.ServingAPIProto;
import feast.proto.types.ValueProto;

public class RangeQueryModel {
  private ValueProto.Value rangeStart;
  private ValueProto.Value rangeEnd;
  private boolean startInclusive;
  private boolean endInclusive;

  public RangeQueryModel(
      Object rangeStart, Object rangeEnd, boolean inclusiveStart, boolean inclusiveEnd) {
    this.rangeStart = RequestUtil.objectToValue(rangeStart);
    this.rangeEnd = RequestUtil.objectToValue(rangeEnd);
    this.startInclusive = inclusiveStart;
    this.endInclusive = inclusiveEnd;
  }

  public ServingAPIProto.SortKeyFilter.RangeQuery toProto() {
    return ServingAPIProto.SortKeyFilter.RangeQuery.newBuilder()
        .setRangeStart(rangeStart)
        .setRangeEnd(rangeEnd)
        .setStartInclusive(startInclusive)
        .setEndInclusive(endInclusive)
        .build();
  }
}
