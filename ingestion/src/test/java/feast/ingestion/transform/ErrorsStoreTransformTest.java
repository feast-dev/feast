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
// import static feast.ingestion.model.Errors.toError;
// import static feast.store.MockFeatureErrorsFactory.MOCK_ERRORS_STORE_TYPE;
// import static feast.store.errors.logging.StderrFeatureErrorsFactory.TYPE_STDERR;
// import static feast.store.errors.logging.StdoutFeatureErrorsFactory.TYPE_STDOUT;
// import static org.junit.Assert.assertEquals;
//
// import feast.ingestion.model.Specs;
// import feast.ingestion.options.ImportJobPipelineOptions;
// import feast.specs.ImportJobSpecsProto.ImportJobSpecs;
// import feast.specs.StorageSpecProto.StorageSpec;
// import feast.store.MockFeatureErrorsFactory;
// import feast.store.errors.FeatureErrorsFactoryService;
// import feast.types.FeatureRowExtendedProto.Attempt;
// import feast.types.FeatureRowExtendedProto.Error;
// import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
// import java.io.File;
// import java.io.IOException;
// import java.nio.file.Files;
// import java.nio.file.Path;
// import java.nio.file.Paths;
// import java.util.Arrays;
// import java.util.List;
// import java.util.stream.Collectors;
// import java.util.stream.Stream;
// import lombok.extern.slf4j.Slf4j;
// import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
// import org.apache.beam.sdk.options.PipelineOptionsFactory;
// import org.apache.beam.sdk.testing.PAssert;
// import org.apache.beam.sdk.testing.TestPipeline;
// import org.apache.beam.sdk.transforms.Create;
// import org.apache.beam.sdk.transforms.Flatten;
// import org.apache.beam.sdk.values.PCollection;
// import org.apache.beam.sdk.values.PCollectionList;
// import org.junit.Before;
// import org.junit.Rule;
// import org.junit.Test;
// import org.junit.rules.TemporaryFolder;
//
// @Slf4j
// public class ErrorsStoreTransformTest {
//
//   @Rule
//   public TemporaryFolder tempFolder = new TemporaryFolder();
//
//   @Rule
//   public TestPipeline pipeline = TestPipeline.create();
//
//   private ImportJobPipelineOptions options;
//   private PCollection<FeatureRowExtended> inputs;
//   private List<FeatureRowExtended> errors;
//
//   public Specs getSpecs(String errorsStorageType) {
//     return new Specs("test", ImportJobSpecs.newBuilder()
//         .setErrorsStorageSpec(StorageSpec.newBuilder()
//             .setId("ERRORS")
//             .setType(errorsStorageType)).build());
//   }
//
//   @Before
//   public void setUp() {
//     options = PipelineOptionsFactory.create().as(ImportJobPipelineOptions.class);
//     options.setJobName("test");
//
//     errors =
//         Arrays.asList(
//             errorOf("test", new Exception("err")), errorOf("test", new Exception("err2")));
//     inputs = pipeline.apply(Create.of(errors)).setCoder(ProtoCoder.of(FeatureRowExtended.class));
//   }
//
//   private FeatureRowExtended errorOf(String transform, Throwable cause) {
//     Error error = toError(transform, cause);
//     return FeatureRowExtended.newBuilder()
//         .setLastAttempt(Attempt.newBuilder().setError(error).build())
//         .build();
//   }
//
//   @Test
//   public void shouldWriteToGivenErrorsStore() {
//     ErrorsStoreTransform transform = new ErrorsStoreTransform(
//         FeatureErrorsFactoryService.getAll(),
//         getSpecs(MOCK_ERRORS_STORE_TYPE), options);
//     transform.expand(inputs);
//
//     MockFeatureErrorsFactory factory = FeatureErrorsFactoryService
//         .get(MockFeatureErrorsFactory.class);
//
//     PCollection<FeatureRowExtended> writtenToErrors =
//         PCollectionList.of(factory.getWrite().getInputs())
//             .apply("flatten errors input", Flatten.pCollections());
//
//     PAssert.that(writtenToErrors).containsInAnyOrder(errors);
//     pipeline.run();
//   }
//
//   @Test
//   public void logErrorsToStdErr() {
//     ErrorsStoreTransform transform = new ErrorsStoreTransform(
//         FeatureErrorsFactoryService.getAll(),
//         getSpecs(TYPE_STDERR), options);
//     inputs.apply(transform);
//     pipeline.run();
//   }
//
//
//   @Test
//   public void logErrorsToStdOut() {
//     ErrorsStoreTransform transform = new ErrorsStoreTransform(
//         FeatureErrorsFactoryService.getAll(),
//         getSpecs(TYPE_STDOUT), options);
//     inputs.apply(transform);
//     pipeline.run();
//   }
//
//   @Test
//   public void logErrorsToWorkspace() throws IOException, InterruptedException {
//     String tempWorkspace = tempFolder.newFolder().toString();
//     options.setWorkspace(tempWorkspace);
//     ErrorsStoreTransform transform = new ErrorsStoreTransform(
//         FeatureErrorsFactoryService.getAll(),
//         getSpecs(""), options);
//     inputs.apply(transform);
//     pipeline.run().waitUntilFinish();
//
//     File dir = new File(tempWorkspace);
//
//     File[] errorsDirs = dir.listFiles((d, name) -> name.startsWith("errors-"));
//     int lineCount = Files.list(Paths.get(tempWorkspace)
//         .resolve(errorsDirs[0].getName()) // errors workspace dir
//         .resolve("test") // test entity dir
//     ).flatMap(path -> {
//       try {
//         return Files.readAllLines(path).stream();
//       } catch (IOException e) {
//         throw new RuntimeException();
//       }
//     }).collect(Collectors.toList()).size();
//     assertEquals(2, lineCount);
//   }
// }
//
