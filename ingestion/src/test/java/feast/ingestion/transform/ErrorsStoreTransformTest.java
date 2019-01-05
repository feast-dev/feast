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

import feast.ingestion.model.Specs;
import feast.ingestion.options.ImportJobOptions;
import feast.storage.MockErrorsStore;
import feast.types.FeatureRowExtendedProto.Attempt;
import feast.types.FeatureRowExtendedProto.Error;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static feast.ingestion.model.Errors.toError;
import static feast.storage.MockErrorsStore.MOCK_ERRORS_STORE_TYPE;

public class ErrorsStoreTransformTest {
  private ImportJobOptions options;
  private Specs specs;
  private PCollection<FeatureRowExtended> inputs;
  private List<FeatureRowExtended> errors;

  @Before
  public void setUp() {
    options = PipelineOptionsFactory.create().as(ImportJobOptions.class);
    options.setJobName("test");
    specs = Specs.builder().jobName("test").build();

    Pipeline p = TestPipeline.create();
    errors =
        Arrays.asList(
            errorOf("test", new Exception("err")), errorOf("test", new Exception("err2")));
    inputs = p.apply(Create.of(errors)).setCoder(ProtoCoder.of(FeatureRowExtended.class));
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
    ErrorsStoreTransform transform = new ErrorsStoreTransform(options, specs, mockStore);
    transform.expand(inputs);

    PCollection<FeatureRowExtended> writtenToErrors =
        PCollectionList.of(mockStore.getWrite().getInputs())
            .apply("flatten errors input", Flatten.pCollections());

    PAssert.that(writtenToErrors).containsInAnyOrder(errors);
  }
}
