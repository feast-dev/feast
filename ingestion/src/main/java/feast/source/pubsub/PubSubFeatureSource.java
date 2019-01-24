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

package feast.source.pubsub;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import com.google.common.base.Strings;
import feast.ingestion.transform.fn.FilterFeatureRowDoFn;
import feast.options.Options;
import feast.options.OptionsParser;
import feast.source.FeatureSource;
import feast.source.FeatureSourceFactory;
import feast.specs.ImportSpecProto.Field;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.types.FeatureRowProto.FeatureRow;
import java.util.ArrayList;
import java.util.List;
import javax.validation.constraints.AssertTrue;
import lombok.Builder;
import lombok.NonNull;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;

/**
 * Transform for reading {@link feast.types.FeatureRowProto.FeatureRow FeatureRow} proto messages
 * from Cloud PubSub.
 *
 * <p>This transform accepts multiple entities in the import spec and expects no columns to be
 * specified as it does not need to construct FeatureRows, merely pass them on.
 *
 * <p>Because Feast ingestion is stateless, the message event time is simply the processing time,
 * there is no need to override it based on any property of the message.
 */
@Builder
public class PubSubFeatureSource extends FeatureSource {

  public static final String PUBSUB_FEATURE_SOURCE_TYPE = "pubsub";

  @NonNull private ImportSpec importSpec;

  @Override
  public PCollection<FeatureRow> expand(PInput input) {
    checkArgument(importSpec.getType().equals(PUBSUB_FEATURE_SOURCE_TYPE));
    PubSubReadOptions options =
        OptionsParser.parse(importSpec.getOptionsMap(), PubSubReadOptions.class);

    PubsubIO.Read<FeatureRow> read = readProtos();

    if (!Strings.isNullOrEmpty(options.subscription)) {
      read = read.fromSubscription(options.subscription);
    } else if (!Strings.isNullOrEmpty(options.topic)) {
      read = read.fromTopic(options.topic);
    }
    PCollection<FeatureRow> featureRow =  input.getPipeline().apply(read);

    if (options.discardUnknownFeatures) {
      List<String> featureIds = new ArrayList<>();
      for(Field field: importSpec.getSchema().getFieldsList()) {
        String featureId = field.getFeatureId();
        if (!Strings.isNullOrEmpty(featureId)) {
          featureIds.add(featureId);
        }
      }
      return featureRow.apply(ParDo.of(new FilterFeatureRowDoFn(featureIds)));
    }
    return featureRow;
  }

  PubsubIO.Read<FeatureRow> readProtos() {
    return PubsubIO.readProtos(FeatureRow.class);
  }

  public static class PubSubReadOptions implements Options {
    public String subscription;
    public String topic;

    @AssertTrue(message = "subscription or topic must be set")
    boolean isValid() {
      return !Strings.isNullOrEmpty(subscription) || !Strings.isNullOrEmpty(topic);
    }
    public boolean discardUnknownFeatures = true;
  }

  @AutoService(FeatureSourceFactory.class)
  public static class Factory implements FeatureSourceFactory {
    @Override
    public String getType() {
      return PUBSUB_FEATURE_SOURCE_TYPE;
    }

    @Override
    public FeatureSource create(ImportSpec importSpec) {
      checkArgument(importSpec.getType().equals(getType()));
      return new PubSubFeatureSource(importSpec);
    }
  }
}
