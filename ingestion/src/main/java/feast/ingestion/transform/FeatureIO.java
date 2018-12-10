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

import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import feast.types.FeatureRowProto.FeatureRow;
import lombok.AllArgsConstructor;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;

public class FeatureIO {

  public abstract static class Read extends PTransform<PInput, PCollection<FeatureRow>> {}

  public abstract static class Write extends PTransform<PCollection<FeatureRowExtended>, PDone> {}

  /** Used during setup if read transform can not be determined. */
  @AllArgsConstructor
  public static class UnknownRead extends Read {
    private String message;

    @Override
    public PCollection<FeatureRow> expand(PInput input) {
      throw new IllegalArgumentException(message);
    }
  }
}
