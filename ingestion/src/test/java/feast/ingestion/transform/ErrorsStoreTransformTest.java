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

import static feast.ingestion.model.Errors.toError;
import static feast.store.MockFeatureErrorsFactory.MOCK_ERRORS_STORE_TYPE;
import static feast.store.errors.logging.StderrFeatureErrorsFactory.TYPE_STDERR;
import static feast.store.errors.logging.StdoutFeatureErrorsFactory.TYPE_STDOUT;
import static org.junit.Assert.assertEquals;

import feast.ingestion.model.Specs;
import feast.ingestion.options.ImportJobPipelineOptions;
import feast.specs.ImportJobSpecsProto.ImportJobSpecs;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.store.MockFeatureErrorsFactory;
import feast.store.errors.FeatureErrorsFactoryService;
import feast.types.FeatureRowExtendedProto.Attempt;
import feast.types.FeatureRowExtendedProto.Error;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

@Slf4j
public class ErrorsStoreTransformTest {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule
  public TestPipeline pipeline = TestPipeline.create();

  private ImportJobPipelineOptions options;
  private PCollection<FeatureRowExtended> inputs;
  private List<FeatureRowExtended> errors;

  public Specs getSpecs(StorageSpec errorsStorageSpec) {
    return new Specs("test", ImportJobSpecs.newBuilder()
        .setErrorsStorageSpec(errorsStorageSpec).build()
    );
  }

  public Specs getSpecs(String errorsStorageType) {
    return new Specs("test", ImportJobSpecs.newBuilder()
        .setErrorsStorageSpec(StorageSpec.newBuilder().setType(errorsStorageType)).build()
    );
  }

  @Before
  public void setUp() {
    options = PipelineOptionsFactory.create().as(ImportJobPipelineOptions.class);
    options.setJobName("test");

    errors =
        Arrays.asList(
            errorOf("test", new Exception("err")), errorOf("test", new Exception("err2")));
    inputs = pipeline.apply(Create.of(errors)).setCoder(ProtoCoder.of(FeatureRowExtended.class));
  }

  private FeatureRowExtended errorOf(String transform, Throwable cause) {
    Error error = toError(transform, cause);
    return FeatureRowExtended.newBuilder()
        .setLastAttempt(Attempt.newBuilder().setError(error).build())
        .build();
  }

  @Test
  public void shouldWriteToGivenErrorsStore() {
    MockFeatureErrorsFactory mockStore = new MockFeatureErrorsFactory();
    ErrorsStoreTransform transform =
        new ErrorsStoreTransform(options, getSpecs(MOCK_ERRORS_STORE_TYPE),
            Lists.newArrayList(mockStore));
    transform.expand(inputs);

    PCollection<FeatureRowExtended> writtenToErrors =
        PCollectionList.of(mockStore.getWrite().getInputs())
            .apply("flatten errors input", Flatten.pCollections());

    PAssert.that(writtenToErrors).containsInAnyOrder(errors);
    pipeline.run();
  }

  @Test
  public void logErrorsToStdErr() {
    ErrorsStoreTransform transform =
        new ErrorsStoreTransform(options, getSpecs(TYPE_STDERR),
            FeatureErrorsFactoryService.getAll());
    inputs.apply(transform);
    pipeline.run();
  }


  @Test
  public void logErrorsToStdOut() {
    ErrorsStoreTransform transform =
        new ErrorsStoreTransform(options, getSpecs(TYPE_STDOUT),
            FeatureErrorsFactoryService.getAll());
    inputs.apply(transform);
    pipeline.run();
  }

  @Test
  public void logErrorsToWorkspace() throws IOException, InterruptedException {
    String tempWorkspace = tempFolder.newFolder().toString();
    options.setWorkspace(tempWorkspace);
    ErrorsStoreTransform transform =
        new ErrorsStoreTransform(options, getSpecs(""), FeatureErrorsFactoryService.getAll());
    inputs.apply(transform);
    pipeline.run().waitUntilFinish();

    int lineCount = Files.list(Paths.get(tempWorkspace)
        .resolve("errors") // errors workspace dir
        .resolve("test") // test entity dir
    ).flatMap(path -> {
      try {
        return Files.readAllLines(path).stream();
      } catch (IOException e) {
        throw new RuntimeException();
      }
    }).collect(Collectors.toList()).size();
    assertEquals(2, lineCount);
  }
}

