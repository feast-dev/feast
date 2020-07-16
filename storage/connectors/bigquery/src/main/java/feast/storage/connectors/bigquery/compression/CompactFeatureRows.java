/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
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
 */
package feast.storage.connectors.bigquery.compression;

import feast.proto.types.FeatureRowProto;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SnappyCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * Collects batch of FeatureRow, converts them into columnar form:
 *
 * <p>List&lt;FeatureRow&gt; -&gt;
 *
 * <p>- Feature1 List&lt;Integer&gt; - all values from all rows in one list
 *
 * <p>- Feature2 List&lt;String&gt;
 *
 * <p>- ...
 *
 * <p>FeatureRowsBatch is in column form, hence it is better compressible. For compression we use
 * Snappy, since it's already available in Beam as SnappyCoder.
 */
public class CompactFeatureRows
    extends PTransform<
        PCollection<KV<String, FeatureRowProto.FeatureRow>>,
        PCollection<KV<String, FeatureRowsBatch>>> {
  private final int batchSize;

  public CompactFeatureRows(int batchSize) {
    this.batchSize = batchSize;
  }

  @Override
  public PCollection<KV<String, FeatureRowsBatch>> expand(
      PCollection<KV<String, FeatureRowProto.FeatureRow>> input) {
    return input
        .apply(GroupIntoBatches.ofSize(batchSize))
        .apply(ParDo.of(new FeatureRowToColumnar()))
        .setCoder(
            KvCoder.of(
                StringUtf8Coder.of(), SnappyCoder.of(FeatureRowsBatch.FeatureRowsCoder.of())));
  }

  public static class FeatureRowToColumnar
      extends DoFn<KV<String, Iterable<FeatureRowProto.FeatureRow>>, KV<String, FeatureRowsBatch>> {
    @ProcessElement
    public void process(ProcessContext c) {
      c.output(KV.of(c.element().getKey(), new FeatureRowsBatch(c.element().getValue())));
    }
  }
}
