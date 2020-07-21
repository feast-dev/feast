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
package feast.ingestion.transform;

import static feast.ingestion.utils.SpecUtil.parseFeatureSetReference;

import com.google.auto.value.AutoValue;
import feast.common.models.Store;
import feast.proto.core.StoreProto;
import feast.proto.types.FeatureRowProto;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.commons.lang3.tuple.Pair;

/**
 * For each incoming {@link FeatureRowProto.FeatureRow} allocator choose only stores that
 * subscripted to its project and featureSet names.
 *
 * <p>Return PCollectionTuple with one {@link TupleTag} per {@link StoreProto.Store}. Tags must be
 * generated in advance.
 */
@AutoValue
public abstract class FeatureRowToStoreAllocator
    extends PTransform<PCollection<FeatureRowProto.FeatureRow>, PCollectionTuple> {
  public abstract List<StoreProto.Store> getStores();

  public abstract Map<StoreProto.Store, TupleTag<FeatureRowProto.FeatureRow>> getStoreTags();

  public static Builder newBuilder() {
    return new AutoValue_FeatureRowToStoreAllocator.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setStores(List<StoreProto.Store> stores);

    public abstract Builder setStoreTags(
        Map<StoreProto.Store, TupleTag<FeatureRowProto.FeatureRow>> tags);

    public abstract FeatureRowToStoreAllocator build();
  }

  @Override
  public PCollectionTuple expand(PCollection<FeatureRowProto.FeatureRow> input) {
    return input.apply(
        "AssignRowToStore",
        ParDo.of(
                new DoFn<FeatureRowProto.FeatureRow, FeatureRowProto.FeatureRow>() {
                  @ProcessElement
                  public void process(ProcessContext c, @Element FeatureRowProto.FeatureRow row) {
                    Pair<String, String> projectAndSetNames =
                        parseFeatureSetReference(row.getFeatureSet());
                    getStores().stream()
                        .filter(
                            s ->
                                Store.isSubscribedToFeatureSet(
                                    s.getSubscriptionsList(),
                                    projectAndSetNames.getLeft(),
                                    projectAndSetNames.getRight()))
                        .forEach(s -> c.output(getStoreTags().get(s), row));
                  }
                })
            .withOutputTags(
                getStoreTags().get(getStores().get(0)),
                TupleTagList.of(
                    getStores().stream()
                        .skip(1)
                        .map(getStoreTags()::get)
                        .collect(Collectors.toList()))));
  }
}
