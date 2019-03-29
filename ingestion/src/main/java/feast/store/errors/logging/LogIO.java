/*
 * Copyright 2019 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package feast.store.errors.logging;

import feast.store.FeatureStoreWrite;
import feast.ingestion.transform.fn.LoggerDoFn;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import lombok.AllArgsConstructor;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.event.Level;

public class LogIO {

  @AllArgsConstructor
  public static class Write extends FeatureStoreWrite {

    private Level level;

    @Override
    public PDone expand(PCollection<FeatureRowExtended> input) {
      input.apply("Log to " + level.toString(), ParDo.of(new LoggerDoFn(level)));
      return PDone.in(input.getPipeline());
    }
  }
}
