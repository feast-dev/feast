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

package feast.ingestion.transform;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import feast.ingestion.model.Specs;
import feast.ingestion.transform.FeatureIO.Write;
import feast.ingestion.transform.SplitFeatures.MultiOutputSplit;
import feast.ingestion.values.PFeatureRows;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.storage.FeatureStoreFactory;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

@AllArgsConstructor
@Slf4j
public class SplitOutputByStore extends PTransform<PFeatureRows, PFeatureRows> {

  private Collection<? extends FeatureStoreFactory> stores;
  private SerializableFunction<FeatureSpec, String> selector;
  private Specs specs;

  @Override
  public PFeatureRows expand(PFeatureRows input) {
    Map<String, Write> transforms = getFeatureStoreTransforms();
    Set<String> keys = transforms.keySet();

    log.info(String.format("Splitting on keys = [%s]", String.join(",", keys)));
    MultiOutputSplit<String> splitter = new MultiOutputSplit<>(selector, keys, specs);
    PCollectionTuple splits = input.getMain().apply(splitter);

    Map<TupleTag<FeatureRowExtended>, Write> taggedTransforms = new HashMap<>();
    for (String key : transforms.keySet()) {
      TupleTag<FeatureRowExtended> tag = splitter.getStrategy().getTag(key);
      taggedTransforms.put(tag, transforms.get(key));
    }

    PCollection<FeatureRowExtended> written = splits
        .apply(new WriteTags(taggedTransforms, MultiOutputSplit.MAIN_TAG));
    return new PFeatureRows(
        written,
        input.getErrors());
  }

  private Map<String, FeatureStoreFactory> getStoresMap() {
    Map<String, FeatureStoreFactory> storesMap = new HashMap<>();
    for (FeatureStoreFactory servingStore : stores) {
      storesMap.put(servingStore.getType(), servingStore);
    }
    return storesMap;
  }

  private Map<String, Write> getFeatureStoreTransforms() {
    Map<String, FeatureStoreFactory> storesMap = getStoresMap();
    Map<String, Write> transforms = new HashMap<>();
    Map<String, StorageSpec> storageSpecs = specs.getStorageSpecs();
    for (String storeId : storageSpecs.keySet()) {
      StorageSpec storageSpec = storageSpecs.get(storeId);
      String type = storageSpec.getType();
      if (storesMap.containsKey(type)) {
        transforms.put(storeId, storesMap.get(type).create(storageSpec, specs));
      }
    }
    return transforms;
  }

  /**
   * Writes each pcollection in the tuple to a correspondingly tagged write transform and returns
   * the a union of the written rows.
   *
   * The main tag, is not written to a transform, but is returned. This represents the default for
   * rows which have no associated store to write to but might want to be written down stream (eg,
   * it has no serving store, but does have a warehouse store).
   *
   * Tag in the tuple that are not the main tag and have no transform, will be discarded
   * completely.
   */
  @AllArgsConstructor
  public static class WriteTags extends
      PTransform<PCollectionTuple, PCollection<FeatureRowExtended>> {

    private Map<TupleTag<FeatureRowExtended>, Write> transforms;
    private TupleTag<FeatureRowExtended> mainTag;

    @Override
    public PCollection<FeatureRowExtended> expand(PCollectionTuple tuple) {
      List<PCollection<FeatureRowExtended>> outputList = Lists.newArrayList();
      for (TupleTag<FeatureRowExtended> tag : transforms.keySet()) {
        Write write = transforms.get(tag);
        Preconditions.checkNotNull(write, String.format("Null transform for tag=%s", tag.getId()));
        PCollection<FeatureRowExtended> input = tuple.get(tag);
        input.apply(String.format("Write to %s", tag.getId()), write);
        outputList.add(input);
      }
      // FeatureRows with no matching write transform end up in `input.get(mainTag)` and considered
      // discardible, we return them in the main output so they are considered written, but don't
      // actually write them to any store.
      outputList.add(tuple.get(mainTag));
      return PCollectionList.of(outputList).apply("Flatten main", Flatten.pCollections());
    }
  }
}
