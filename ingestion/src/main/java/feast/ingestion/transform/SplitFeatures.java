// /*
//  * Copyright 2018 The Feast Authors
//  *
//  * Licensed under the Apache License, Version 2.0 (the "License");
//  * you may not use this file except in compliance with the License.
//  * You may obtain a copy of the License at
//  *
//  *     https://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS,
//  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  * See the License for the specific language governing permissions and
//  * limitations under the License.
//  *
//  */
//
// package feast.ingestion.transform;
//
// import com.google.common.collect.Maps;
// import java.io.Serializable;
// import java.util.HashMap;
// import lombok.Getter;
// import org.apache.beam.sdk.transforms.PTransform;
// import org.apache.beam.sdk.transforms.ParDo;
// import org.apache.beam.sdk.transforms.SerializableFunction;
// import org.apache.beam.sdk.values.PCollection;
// import org.apache.beam.sdk.values.PCollectionTuple;
// import org.apache.beam.sdk.values.TupleTag;
// import org.apache.beam.sdk.values.TupleTagList;
// import feast.ingestion.model.Specs;
// import feast.ingestion.transform.fn.SplitFeaturesDoFn;
// import feast.specs.FeatureSpecProto.FeatureSpec;
// import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
//
// public class SplitFeatures {
//
//   @Getter
//   public static class SingleOutputSplit<T>
//       extends PTransform<PCollection<FeatureRowExtended>, PCollection<FeatureRowExtended>> {
//
//     private SplitStrategy<T> strategy;
//     private Specs specs;
//
//     public SingleOutputSplit(SerializableFunction<FeatureSpec, T> splitFn, Specs specs) {
//       this.strategy = new SplitStrategy<>(splitFn);
//       this.specs = specs;
//     }
//
//     @Override
//     public PCollection<FeatureRowExtended> expand(PCollection<FeatureRowExtended> input) {
//       return input.apply(ParDo.of(new SplitFeaturesDoFn<>(strategy, specs)));
//     }
//   }
//
//   @Getter
//   public static class MultiOutputSplit<T>
//       extends PTransform<PCollection<FeatureRowExtended>, PCollectionTuple> {
//
//     public static TupleTag<FeatureRowExtended> MAIN_TAG = new TupleTag<FeatureRowExtended>() {};
//     private SplitStrategy<T> strategy;
//     private Specs specs;
//
//     public MultiOutputSplit(
//         SerializableFunction<FeatureSpec, T> splitFn, Iterable<T> keys, Specs specs) {
//       this.strategy = new SplitStrategy<>(splitFn, keys);
//       this.specs = specs;
//     }
//
//     @Override
//     public PCollectionTuple expand(PCollection<FeatureRowExtended> input) {
//       return input.apply(
//           ParDo.of(new SplitFeaturesDoFn<>(strategy, specs))
//               .withOutputTags(MAIN_TAG, strategy.getTags()));
//     }
//   }
//
//   public static class SplitStrategy<T> implements Serializable {
//
//     private SerializableFunction<FeatureSpec, T> splitFn;
//     private HashMap<String, TupleTag<FeatureRowExtended>> tagMap = Maps.newHashMap();
//
//     public SplitStrategy(SerializableFunction<FeatureSpec, T> splitFn, Iterable<T> keys) {
//       this.splitFn = splitFn;
//       for (T key : keys) {
//         String tagId = getTagId(key);
//         tagMap.put(tagId, new TupleTag<FeatureRowExtended>(tagId) {});
//       }
//     }
//
//     public SplitStrategy(SerializableFunction<FeatureSpec, T> splitFn) {
//       this.splitFn = splitFn;
//     }
//
//     public TupleTagList getTags() {
//       TupleTagList list = TupleTagList.empty();
//       for (TupleTag<FeatureRowExtended> tag : tagMap.values()) {
//         list = list.and(tag);
//       }
//       return list;
//     }
//
//     public TupleTag<FeatureRowExtended> getTag(FeatureSpec featureInfo) {
//       return getTag(splitFn.apply(featureInfo));
//     }
//
//     public TupleTag<FeatureRowExtended> getTag(T key) {
//       return new TupleTag<FeatureRowExtended>(getTagId(key)) {};
//     }
//
//     private String getTagId(T key) {
//       return key.toString();
//     }
//
//     public boolean isOutputTag(TupleTag<FeatureRowExtended> tag) {
//       return tagMap.containsKey(tag.getId());
//     }
//   }
// }
