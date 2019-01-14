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

package feast.storage.bigtable;

import com.google.cloud.bigtable.beam.CloudBigtableConfiguration;
import com.google.cloud.bigtable.beam.CloudBigtableIO;
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.hadoop.hbase.client.Mutation;
import feast.ingestion.model.Specs;
import feast.ingestion.transform.FeatureIO;
import feast.ingestion.transform.SplitFeatures.SingleOutputSplit;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;

@Slf4j
public class FeatureRowBigTableIO {

  public static class Write extends FeatureIO.Write {

    private BigTableStoreOptions bigTableOptions;
    private Specs specs;

    public Write(BigTableStoreOptions bigTableOptions, Specs specs) {
      this.bigTableOptions = bigTableOptions;
      this.specs = specs;
    }

    @Override
    public PDone expand(PCollection<FeatureRowExtended> input) {
      log.info("Using BigTable options: " + bigTableOptions.toString());

      // entity name to mutation key value
      PCollection<KV<String, Mutation>> mutations =
          input.apply(
              "Map to BigTable mutations",
              ParDo.of(new FeatureRowToBigTableMutationDoFn(bigTableOptions.prefix, specs)));

      PCollection<KV<String, Iterable<Mutation>>> iterableMutations =
          mutations.apply(
              ParDo.of(
                  new DoFn<KV<String, Mutation>, KV<String, Iterable<Mutation>>>() {
                    @ProcessElement
                    public void processElement(ProcessContext context) {
                      KV<String, Mutation> kv = context.element();
                      KV<String, Iterable<Mutation>> mutationIters =
                          KV.of(kv.getKey(), Collections.singleton(kv.getValue()));
                      context.output(mutationIters);
                    }
                  }));
      return iterableMutations.apply(
          CloudBigtableIO.writeToMultipleTables(
              new CloudBigtableConfiguration.Builder()
                  .withInstanceId(bigTableOptions.instance)
                  .withProjectId(bigTableOptions.project)
                  .build()));
    }
  }
}
