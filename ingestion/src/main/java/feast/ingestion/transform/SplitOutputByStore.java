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
import feast.ingestion.exceptions.ErrorsHandler;
import feast.ingestion.model.Specs;
import feast.ingestion.transform.FeatureIO.Write;
import feast.ingestion.transform.SplitFeatures.MultiOutputSplit;
import feast.ingestion.values.PFeatureRows;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.storage.FeatureStore;
import feast.storage.noop.NoOpIO;
import feast.types.FeatureRowExtendedProto.Attempt;
import feast.types.FeatureRowExtendedProto.Error;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

@AllArgsConstructor
@Slf4j
public class SplitOutputByStore extends PTransform<PFeatureRows, PFeatureRows> {

  private Collection<? extends FeatureStore> stores;
  private SerializableFunction<FeatureSpec, String> selector;
  private Specs specs;

  @Override
  public PFeatureRows expand(PFeatureRows input) {
    Map<String, Write> transforms = getFeatureStoreTransforms();
    transforms.put("", new NoOpIO.Write());
    Set<String> keys = transforms.keySet();
    Preconditions.checkArgument(transforms.size() > 0, "no write transforms found");

    log.info(String.format("Splitting on keys = [%s]", String.join(",", keys)));
    MultiOutputSplit<String> splitter = new MultiOutputSplit<>(selector, keys, specs);
    PCollectionTuple splits = input.getMain().apply(splitter);

    Map<TupleTag<FeatureRowExtended>, Write> taggedTransforms = new HashMap<>();
    for (String key : transforms.keySet()) {
      TupleTag<FeatureRowExtended> tag = splitter.getStrategy().getTag(key);
      taggedTransforms.put(tag, transforms.get(key));
    }
    PFeatureRows output = splits.apply(new WriteTags(taggedTransforms, MultiOutputSplit.MAIN_TAG));
    return new PFeatureRows(
        output.getMain(),
        PCollectionList.of(input.getErrors())
            .and(output.getErrors())
            .apply("Flatten errors", Flatten.pCollections()));
  }

  private Map<String, FeatureStore> getStoresMap() {
    Map<String, FeatureStore> storesMap = new HashMap<>();
    for (FeatureStore servingStore : stores) {
      storesMap.put(servingStore.getType(), servingStore);
    }
    return storesMap;
  }

  private Map<String, Write> getFeatureStoreTransforms() {
    Map<String, FeatureStore> storesMap = getStoresMap();
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

  @AllArgsConstructor
  public static class WriteTags extends PTransform<PCollectionTuple, PFeatureRows> {

    private Map<TupleTag<FeatureRowExtended>, Write> transforms;
    private TupleTag<FeatureRowExtended> mainTag;

    @Override
    public PFeatureRows expand(PCollectionTuple input) {

      List<PCollection<FeatureRowExtended>> mainList = Lists.newArrayList();
      for (TupleTag<FeatureRowExtended> tag : transforms.keySet()) {
        Write write = transforms.get(tag);
        Preconditions.checkNotNull(write, String.format("Null transform for tag=%s", tag.getId()));
        PCollection<FeatureRowExtended> main = input.get(tag);
        if (!(write instanceof NoOpIO.Write)) {
          main.apply(String.format("Write to %s", tag.getId()), write);
        }
        mainList.add(main);
      }

      String message =
          "FeatureRows have no matching write transform, these rows should not have passed validation.";
      PCollection<FeatureRowExtended> errors =
          input.get(mainTag).apply(ParDo.of(new WithErrors(getName(), message)));

      return new PFeatureRows(
          PCollectionList.of(mainList).apply("Flatten main", Flatten.pCollections()), errors);
    }
  }

  /**
   * Sets the last attempt error for all rows with a given exception
   */
  public static class WithErrors extends DoFn<FeatureRowExtended, FeatureRowExtended> {

    private Error error;

    public WithErrors(Error error) {
      this.error = error;
    }

    public WithErrors(String transformName, String message) {
      this(Error.newBuilder().setTransform(transformName).setMessage(message).build());
    }

    @ProcessElement
    public void processElement(
        @Element FeatureRowExtended rowExtended, OutputReceiver<FeatureRowExtended> out) {
      Attempt lastAttempt = rowExtended.getLastAttempt();

      Error lastError = lastAttempt.getError();
      Error thisError = error;

      int numAttempts =
          ErrorsHandler.checkAttemptCount(lastAttempt.getAttempts(), lastError, thisError);

      Attempt thisAttempt =
          Attempt.newBuilder().setAttempts(numAttempts + 1).setError(thisError).build();
      out.output(
          FeatureRowExtended.newBuilder()
              .mergeFrom(rowExtended)
              .setLastAttempt(thisAttempt)
              .build());
    }
  }
}
