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

package feast.source.bigquery;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import feast.options.Options;
import feast.options.OptionsParser;
import feast.source.FeatureSource;
import feast.source.FeatureSourceFactory;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.types.FeatureRowProto.FeatureRow;
import java.util.List;
import javax.validation.constraints.NotEmpty;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;


/**
 * Transform for processing BigQuery tables and producing FeatureRow messages.
 *
 * <p>This transform asserts that the import spec is for only one entity, as all columns must have
 * the same entity. The columns names in the import spec will be used for selecting columns from the
 * BigQuery table, but it still scans the whole row.
 *
 * <p>The output is a PCollection of {@link feast.types.FeatureRowProto.FeatureRow FeatureRows},
 * where each feature and the entity key {@link feast.types.ValueProto.Value Value} in the
 * FeatureRow is taken from a column in the BigQuery table and set with the closest type to the
 * BigQuery column schema that is available in {@link feast.types.ValueProto.ValueType ValueType}.
 *
 * <p>Note a small gotcha is that because Integers and Numerics in BigQuery are 64 bits, these are
 * always cast to INT64 and DOUBLE respectively.
 *
 * <p>Downstream these will fail validation if the corresponding {@link
 * feast.specs.FeatureSpecProto.FeatureSpec FeatureSpec} has a 32 bit type.
 */
@AllArgsConstructor
public class BigQueryFeatureSource extends FeatureSource {

  private static final String BIGQUERY_FEATURE_SOURCE_TYPE = "bigquery";

  @NonNull
  private final ImportSpec importSpec;

  @Override
  public PCollection<FeatureRow> expand(PInput input) {
    BigQuerySourceOptions options = OptionsParser
        .parse(importSpec.getSourceOptionsMap(), BigQuerySourceOptions.class);

    List<String> entities = importSpec.getEntitiesList();
    Preconditions.checkArgument(
        entities.size() == 1, "exactly 1 entity must be set for BigQuery import");

    String url = String.format("%s:%s.%s", options.project, options.dataset, options.table);

    return input
        .getPipeline()
        .apply(
            BigQueryIO.read(new BigQueryToFeatureRowFn(importSpec)).from(url));
  }


  public static class BigQuerySourceOptions implements Options {

    @NotEmpty
    public String project;
    @NotEmpty
    public String dataset;
    @NotEmpty
    public String table;
  }


  @AutoService(FeatureSourceFactory.class)
  public static class Factory implements FeatureSourceFactory {

    @Override
    public String getType() {
      return BIGQUERY_FEATURE_SOURCE_TYPE;
    }

    @Override
    public FeatureSource create(ImportSpec importSpec) {
      checkArgument(importSpec.getType().equals(getType()));
      return new BigQueryFeatureSource(importSpec);
    }
  }
}
