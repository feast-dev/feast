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
import com.google.inject.Inject;
import feast.source.FeatureSourceFactory;
import feast.source.FeatureSourceFactoryService;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.types.FeatureRowProto.FeatureRow;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;

public class ReadFeaturesTransform extends PTransform<PInput, PCollection<FeatureRow>> {

  private ImportSpec importSpec;

  @Inject
  public ReadFeaturesTransform(ImportSpec importSpec) {
    this.importSpec = importSpec;
  }

  @Override
  public PCollection<FeatureRow> expand(PInput input) {
    return input.getPipeline().apply("Read " + importSpec.getType(), getTransform());
  }

  public PTransform<PInput, PCollection<FeatureRow>> getTransform() {
    String type = importSpec.getType();
    Preconditions.checkArgument(!type.isEmpty(), "type missing in import spec");

    FeatureSourceFactory featureSourceFactory = null;
    for (FeatureSourceFactory factory : FeatureSourceFactoryService.getAll()) {
      if (type.equals(factory.getType())) {
        featureSourceFactory = factory;
      }
    }
    Preconditions
        .checkNotNull(featureSourceFactory, "No FeatureSourceFactory found for type " + type);
    return featureSourceFactory.create(importSpec);
  }
}

