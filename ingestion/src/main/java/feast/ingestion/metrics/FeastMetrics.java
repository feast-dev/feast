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
// package feast.ingestion.metrics;
//
// import com.google.protobuf.util.Timestamps;
// import feast.types.FeatureProto.Feature;
// import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
// import feast.types.FeatureRowProto;
// import feast.types.FeatureRowProto.FeatureRow;
// import lombok.AllArgsConstructor;
// import org.apache.beam.sdk.metrics.Metrics;
// import org.apache.beam.sdk.transforms.DoFn;
//
// public class FeastMetrics {
//
//   public static final String FEAST_NAMESPACE = "feast";
//
//   private FeastMetrics() {}
//
//   private static void inc(String name) {
//     Metrics.counter(FeastMetrics.FEAST_NAMESPACE, name).inc();
//   }
//
//   public static void update(String name, long value) {
//     Metrics.distribution(FeastMetrics.FEAST_NAMESPACE, name).update(value);
//   }
//
//   public static void inc(FeatureRow row, String suffix) {
//     inc("row:" + suffix);
//     inc(String.format("entity:%s:%s", row.getEntityName(), suffix));
//     for (Feature feature : row.getFeaturesList()) {
//       inc(String.format("feature:%s:%s", feature.getId(), suffix));
//     }
//   }
//
//   public static IncrRowExtendedFunc incrDoFn(String suffix) {
//     return new IncrRowExtendedFunc(suffix);
//   }
//
//   public static CalculateLagMetricFunc lagUpdateDoFn() {
//     return new CalculateLagMetricFunc();
//   }
//
//   @AllArgsConstructor
//   public static class IncrRowExtendedFunc extends DoFn<FeatureRowExtended, FeatureRowExtended> {
//
//     private String suffix;
//
//     @ProcessElement
//     public void processElement(
//         @Element FeatureRowExtended element, OutputReceiver<FeatureRowExtended> out) {
//       inc(element.getRow(), suffix);
//       out.output(element);
//     }
//   }
//
//   @AllArgsConstructor
//   public static class CalculateLagMetricFunc extends DoFn<FeatureRowExtended, FeatureRowExtended> {
//
//     @ProcessElement
//     public void processElement(
//         @Element FeatureRowExtended element, OutputReceiver<FeatureRowExtended> out) {
//       FeatureRowProto.FeatureRow row = element.getRow();
//       com.google.protobuf.Timestamp eventTimestamp = row.getEventTimestamp();
//
//       com.google.protobuf.Timestamp now = Timestamps.fromMillis(System.currentTimeMillis());
//       long lagSeconds = now.getSeconds() - eventTimestamp.getSeconds();
//       FeastMetrics.update("row:lag", lagSeconds);
//       out.output(element);
//     }
//   }
// }
