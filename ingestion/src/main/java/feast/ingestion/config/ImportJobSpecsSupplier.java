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
// package feast.ingestion.config;
//
// import com.google.common.base.Strings;
// import feast.ingestion.util.PathUtil;
// import feast.ingestion.util.ProtoUtil;
// import feast.specs.ImportJobSpecsProto.ImportJobSpecs;
// import java.io.IOException;
// import java.nio.file.Path;
// import java.util.function.Supplier;
//
// public class ImportJobSpecsSupplier implements Supplier<ImportJobSpecs> {
//
//   public static final String IMPORT_JOB_SPECS_FILENAME = "importJobSpecs.yaml";
//
//   private Path importJobSpecsPath;
//   private ImportJobSpecs importJobSpecs;
//
//   public ImportJobSpecsSupplier(String workspace) {
//     if (!Strings.isNullOrEmpty(workspace)) {
//       this.importJobSpecsPath = PathUtil.getPath(workspace).resolve(IMPORT_JOB_SPECS_FILENAME);
//     }
//   }
//
//   private ImportJobSpecs create() {
//     try {
//       if (importJobSpecsPath != null) {
//         return ProtoUtil
//             .decodeProtoYamlFile(importJobSpecsPath, ImportJobSpecs.getDefaultInstance());
//       } else {
//         return ImportJobSpecs.getDefaultInstance();
//       }
//     } catch (IOException e) {
//       throw new RuntimeException(e);
//     }
//   }
//
//   @Override
//   public ImportJobSpecs get() {
//     if (importJobSpecs == null) {
//       importJobSpecs = create();
//     }
//     return importJobSpecs;
//   }
// }
