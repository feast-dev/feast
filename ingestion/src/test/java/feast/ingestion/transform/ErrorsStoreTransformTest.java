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
import static feast.storage.MockErrorsStore.MOCK_ERRORS_STORE_TYPE;

import feast.ingestion.model.Specs;
import feast.ingestion.options.ImportJobPipelineOptions;
import feast.storage.MockErrorsStore;
import feast.storage.service.ErrorsStoreService;
import feast.storage.stderr.StderrErrorsStore;
import feast.storage.stderr.StdoutErrorsStore;
import feast.types.FeatureRowExtendedProto.Attempt;
import feast.types.FeatureRowExtendedProto.Error;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import java.util.Arrays;
import java.util.List;
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

public class ErrorsStoreTransformTest {

  @Rule
  public TestPipeline pipeline = TestPipeline.create();

  private ImportJobPipelineOptions options;
  private Specs specs;
  private PCollection<FeatureRowExtended> inputs;
  private List<FeatureRowExtended> errors;

  @Before
  public void setUp() {
    options = PipelineOptionsFactory.create().as(ImportJobPipelineOptions.class);
    options.setJobName("test");
    specs = Specs.builder().jobName("test").build();

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
    MockErrorsStore mockStore = new MockErrorsStore();
    options.setErrorsStoreType(MOCK_ERRORS_STORE_TYPE);
    ErrorsStoreTransform transform =
        new ErrorsStoreTransform(options, specs, Lists.newArrayList(mockStore));
    transform.expand(inputs);

    PCollection<FeatureRowExtended> writtenToErrors =
        PCollectionList.of(mockStore.getWrite().getInputs())
            .apply("flatten errors input", Flatten.pCollections());

    PAssert.that(writtenToErrors).containsInAnyOrder(errors);
    pipeline.run();
  }

  @Test
  public void logErrorsToStdErr() {
    options.setErrorsStoreType(StderrErrorsStore.TYPE_STDERR);
    ErrorsStoreTransform transform =
        new ErrorsStoreTransform(options, specs, ErrorsStoreService.getAll());
    inputs.apply(transform);
    pipeline.run();
  }


  @Test
  public void logErrorsToStdOut() {
    options.setErrorsStoreType(StdoutErrorsStore.TYPE_STDOUT);
    ErrorsStoreTransform transform =
        new ErrorsStoreTransform(options, specs, ErrorsStoreService.getAll());
    inputs.apply(transform);
    pipeline.run();
  }

  @Test
  public void logToNull() {
    //options.setErrorsStoreType(...); // no errors store type set
    ErrorsStoreTransform transform =
        new ErrorsStoreTransform(options, specs, ErrorsStoreService.getAll());
    inputs.apply(transform);
    pipeline.run();
  }
}

