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
package feast.ingestion.transform.specs;

import feast.common.models.Store;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.SourceProto;
import feast.proto.core.StoreProto;
import java.util.List;
import org.apache.beam.sdk.transforms.ProcessFunction;
import org.apache.beam.sdk.values.KV;

/**
 * Selects only those FeatureSetSpecs that have the same source as current Job and their featureSet
 * reference matches to current Job Subscriptions (extracted from Stores)
 */
public class FilterRelevantFunction
    implements ProcessFunction<KV<String, FeatureSetProto.FeatureSetSpec>, Boolean> {
  private final List<StoreProto.Store> stores;
  private final SourceProto.Source source;

  public FilterRelevantFunction(SourceProto.Source source, List<StoreProto.Store> stores) {
    this.source = source;
    this.stores = stores;
  }

  @Override
  public Boolean apply(KV<String, FeatureSetProto.FeatureSetSpec> input) throws Exception {
    return stores.stream()
            .anyMatch(
                s ->
                    Store.isSubscribedToFeatureSet(
                        s.getSubscriptionsList(),
                        input.getValue().getProject(),
                        input.getValue().getName()))
        && input.getValue().getSource().equals(source);
  }
}
