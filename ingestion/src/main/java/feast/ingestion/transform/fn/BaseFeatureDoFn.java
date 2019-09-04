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
// package feast.ingestion.transform.fn;
//
// import com.google.common.base.Strings;
// import org.apache.beam.sdk.transforms.DoFn;
// import feast.ingestion.exceptions.ErrorsHandler;
// import feast.ingestion.metrics.FeastMetrics;
// import feast.ingestion.model.Errors;
// import feast.types.FeatureRowExtendedProto.Error;
// import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
//
// public abstract class BaseFeatureDoFn extends DoFn<FeatureRowExtended, FeatureRowExtended> {
//
//   private String transformName;
//
//   public BaseFeatureDoFn withTransformName(String transformName) {
//     this.transformName = transformName;
//     return this;
//   }
//
//   @ProcessElement
//   public void baseProcessElement(ProcessContext context) {
//     try {
//       processElementImpl(context);
//     } catch (Throwable throwable) {
//       FeastMetrics.inc(context.element().getRow(), "error");
//       String message =
//           Strings.isNullOrEmpty(throwable.getMessage())
//               ? "Error during process element"
//               : throwable.getMessage();
//       Error error = Errors.toError(transformName, message, throwable);
//       ErrorsHandler.handleError(context, context.element(), error);
//     }
//   }
//
//   public abstract  void processElementImpl(ProcessContext context);
// }
