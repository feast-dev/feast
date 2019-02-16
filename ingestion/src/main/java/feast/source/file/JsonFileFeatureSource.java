/*
 * Copyright 2019 The Feast Authors
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

package feast.source.file;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import feast.options.Options;
import feast.options.OptionsParser;
import feast.source.FeatureSource;
import feast.source.FeatureSourceFactory;
import feast.source.common.ParseJsonTransform;
import feast.source.common.ValueMapToFeatureRowTransform;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.specs.ImportSpecProto.Schema;
import feast.types.FeatureRowProto.FeatureRow;
import javax.validation.constraints.NotEmpty;
import lombok.AllArgsConstructor;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;


/**
 * Transform for processing JSON text jsonfiles and that contain JSON serialised FeatureRow
 * messages, one per line.
 *
 * <p>This transform asserts that the import spec is for only one entity, as all columns must
 * have the same entity.
 *
 * <p>The output is a PCollection of {@link feast.types.FeatureRowProto.FeatureRow FeatureRows}.
 */
@AllArgsConstructor
public class JsonFileFeatureSource extends FeatureSource {

  public static final String JSON_FILE_FEATURE_SOURCE_TYPE = "file.json";

  private final ImportSpec importSpec;

  @Override
  public PCollection<FeatureRow> expand(PInput input) {
    JsonFileFeatureSourceOptions options = OptionsParser
        .parse(importSpec.getSourceOptionsMap(), JsonFileFeatureSourceOptions.class);
    PCollection<String> jsonLines = input.getPipeline().apply(TextIO.read().from(options.path));

    Preconditions
        .checkArgument(importSpec.getEntitiesCount() == 1, "Import spec must have only 1 entity");
    String entity = importSpec.getEntities(0);
    Schema schema = importSpec.getSchema();

    return jsonLines.apply(new ParseJsonTransform())
        .apply(new ValueMapToFeatureRowTransform(entity, schema));
  }

  public static class JsonFileFeatureSourceOptions implements Options {

    @NotEmpty
    public String path;
  }

  @AutoService(FeatureSourceFactory.class)
  public static class Factory implements FeatureSourceFactory {

    @Override
    public String getType() {
      return JSON_FILE_FEATURE_SOURCE_TYPE;
    }

    @Override
    public FeatureSource create(ImportSpec importSpec) {
      checkArgument(importSpec.getType().equals(getType()));

      return new JsonFileFeatureSource(importSpec);
    }
  }

}

