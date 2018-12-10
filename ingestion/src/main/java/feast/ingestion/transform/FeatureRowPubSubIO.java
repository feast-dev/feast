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

import static com.google.common.base.Preconditions.checkArgument;

import javax.validation.constraints.AssertTrue;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import feast.options.Options;
import feast.options.OptionsParser;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.types.FeatureRowProto.FeatureRow;

public class FeatureRowPubSubIO {

  public static final String PUB_SUB_TYPE = "pubsub";

  public static Read read(ImportSpec importSpec) {
    return new Read(importSpec);
  }

  /**
   * Transform for reading {@link feast.types.FeatureRowProto.FeatureRow FeatureRow}
   * proto messages from Cloud PubSub.
   *
   * <p>This transform accepts multiple entities in the import spec and expects no columns to be
   * specified as it does not need to construct FeatureRows, merely pass them on.
   *
   * <p>Because Feast ingestion is stateless, the message event time is simply the processing time,
   * there is no need to override it based on any property of the message.
   */
  public static class Read extends FeatureIO.Read {

    private ImportSpec importSpec;

    private Read(ImportSpec importSpec) {
      this.importSpec = importSpec;
    }

    @Override
    public PCollection<FeatureRow> expand(PInput input) {
      checkArgument(importSpec.getType().equals(PUB_SUB_TYPE));
      PubSubReadOptions options =
          OptionsParser.parse(importSpec.getOptionsMap(), PubSubReadOptions.class);

      PubsubIO.Read<FeatureRow> read = readProtos();

      if (options.subscription != null) {
        read = read.fromSubscription(options.subscription);
      } else if (options.topic != null) {
        read = read.fromTopic(options.topic);
      }
      return input.getPipeline().apply(read);
    }

    PubsubIO.Read<FeatureRow> readProtos() {
      return PubsubIO.readProtos(FeatureRow.class);
    }

    public static class PubSubReadOptions implements Options {
      public String subscription;
      public String topic;

      @AssertTrue(message = "subscription or topic must be set")
      boolean isValid() {
        return subscription != null || topic != null;
      }
    }
  }
}
