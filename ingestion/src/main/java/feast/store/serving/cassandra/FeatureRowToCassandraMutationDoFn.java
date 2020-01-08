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
package feast.store.serving.cassandra;

import com.google.protobuf.Duration;
import com.google.protobuf.util.Timestamps;
import feast.core.FeatureSetProto.FeatureSet;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;

public class FeatureRowToCassandraMutationDoFn extends DoFn<FeatureRow, CassandraMutation> {

  private static final Logger log =
      org.slf4j.LoggerFactory.getLogger(FeatureRowToCassandraMutationDoFn.class);
  private Map<String, FeatureSet> featureSets;
  private Map<String, Integer> maxAges;

  public FeatureRowToCassandraMutationDoFn(Map<String, FeatureSet> featureSets, Duration defaultTtl) {
    this.featureSets = featureSets;
    this.maxAges = new HashMap<>();
    for (FeatureSet set : featureSets.values()) {
      FeatureSetSpec spec = set.getSpec();
      String featureSetName = spec.getName() + ":" + spec.getVersion();
      if (spec.getMaxAge() != null && spec.getMaxAge().getSeconds() > 0) {
        maxAges.put(featureSetName, Math.toIntExact(spec.getMaxAge().getSeconds()));
      } else {
        maxAges.put(featureSetName, Math.toIntExact(defaultTtl.getSeconds()));
      }
    }
  }

  /** Output a Cassandra mutation object for every feature in the feature row. */
  @ProcessElement
  public void processElement(ProcessContext context) {
    FeatureRow featureRow = context.element();
    try {
      FeatureSetSpec featureSetSpec = featureSets.get(featureRow.getFeatureSet()).getSpec();
      Set<String> featureNames =
          featureSetSpec.getFeaturesList().stream()
              .map(FeatureSpec::getName)
              .collect(Collectors.toSet());
      String key = CassandraMutation.keyFromFeatureRow(featureSetSpec, featureRow);

      Collection<CassandraMutation> mutations = new ArrayList<>();
      for (Field field : featureRow.getFieldsList()) {
        if (featureNames.contains(field.getName())) {
          mutations.add(
              new CassandraMutation(
                  key,
                  field.getName(),
                  ByteBuffer.wrap(field.getValue().toByteArray()),
                  Timestamps.toMicros(featureRow.getEventTimestamp()),
                  maxAges.get(featureRow.getFeatureSet())));
        }
      }

      mutations.forEach(context::output);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }
  }
}
