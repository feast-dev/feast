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
package feast;

import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedBytes;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto.Field;
import java.util.List;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

public class ToOrderedFeatureRows
    extends PTransform<PCollection<FeatureRowExtended>, PCollection<FeatureRow>> {

  public static FeatureRow orderedFeatureRow(FeatureRow row) {
    List<Field> features = Lists.newArrayList(row.getFieldsList());
    features.sort(
        (f1, f2) ->
            UnsignedBytes.lexicographicalComparator().compare(f1.toByteArray(), f2.toByteArray()));
    return row.toBuilder().clearFields().addAllFields(features).build();
  }

  @Override
  public PCollection<FeatureRow> expand(PCollection<FeatureRowExtended> input) {
    return input
        .apply(
            "get rows",
            MapElements.into(TypeDescriptor.of(FeatureRow.class)).via(FeatureRowExtended::getRow))
        .apply(
            "normalize rows",
            MapElements.into(TypeDescriptor.of(FeatureRow.class))
                .via(ToOrderedFeatureRows::orderedFeatureRow));
  }
}
