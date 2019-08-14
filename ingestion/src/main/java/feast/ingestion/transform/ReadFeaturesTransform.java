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
import feast.source.kafka.KafkaFeatureSource;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.ImportJobSpecsProto.ImportJobSpecs;
import feast.specs.ImportJobSpecsProto.SourceSpec;
import feast.specs.ImportJobSpecsProto.SourceSpec.SourceType;
import feast.types.FeatureRowProto.FeatureRow;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;

import java.util.ArrayList;
import java.util.List;

public class ReadFeaturesTransform extends PTransform<PInput, PCollection<FeatureRow>> {

  // private SourceSpec sourceSpec;
  // private List<String> featureIds;

  private SourceSpec sourceSpec;
  private List<String> featureIds = new ArrayList<>();

  @Inject
  public ReadFeaturesTransform(ImportJobSpecs importJobSpecs) {
    // this.sourceSpec = specs.getSourceSpec();
    // this.featureIds = Lists.newArrayList(specs.getFeatureSpecs().keySet());

    this.sourceSpec = importJobSpecs.getSourceSpec();
    for (FeatureSpec featureSpec : importJobSpecs.getFeatureSpecsList()) {
      featureIds.add(featureSpec.getId());
    }
  }

  @Override
  public PCollection<FeatureRow> expand(PInput input) {
    return input.getPipeline().apply("Read from " + sourceSpec.getType().name(), getTransform());
  }

  public PTransform<PInput, PCollection<FeatureRow>> getTransform() {
    if (!sourceSpec.getType().equals(SourceType.KAFKA)) {
      throw new IllegalArgumentException("Only SourceType.KAFKA is supported as the source for FeatureRow in Feast 0.2");
    }
    return new KafkaFeatureSource(sourceSpec, featureIds);

    //SourceType sourceType = sourceSpec.getType();
    // FeatureSourceFactory featureSourceFactory = null;
    // for (FeatureSourceFactory factory : FeatureSourceFactoryService.getAll()) {
    //   if (sourceType.equals(factory.getType())) {
    //     featureSourceFactory = factory;
    //   }
    // }
    // Preconditions.checkNotNull(
    //     featureSourceFactory, "No FeatureSourceFactory found for type " + sourceType);
    // return featureSourceFactory.create(sourceSpec, featureIds);
  }
}
