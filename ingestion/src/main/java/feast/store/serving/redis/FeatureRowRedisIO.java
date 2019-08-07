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

package feast.store.serving.redis;

import feast.ingestion.model.Specs;
import feast.store.FeatureStoreWrite;
import feast.store.serving.redis.RedisCustomIO.RedisMutation;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

public class FeatureRowRedisIO {

  public static class Write extends FeatureStoreWrite {

    private final RedisStoreOptions options;
    private final Specs specs;

    public Write(RedisStoreOptions options, Specs specs) {
      this.options = options;
      this.specs = specs;
    }

    @Override
    public PDone expand(PCollection<FeatureRowExtended> input) {
      // TODO: Remove this?
      // PCollection<RedisMutation> mutation =
      //     input.apply("Map to Redis mutations", ParDo.of(new FeatureRowToRedisMutationDoFn(specs)));
      // return mutation.apply(
      //     RedisCustomIO.write(options.host, options.port)
      //         .withBatchSize(options.batchSize)
      //         .withTimeout(options.timeout));
      return PDone.in(input.getPipeline());
    }
  }
}
