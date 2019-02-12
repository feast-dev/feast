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

package feast.source.file;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import feast.options.Options;
import feast.options.OptionsParser;
import feast.source.FeatureSource;
import feast.source.FeatureSourceFactory;
import feast.source.common.ParseCsvTransform;
import feast.source.common.StringToValueMapTransform;
import feast.source.common.ValueMapToFeatureRowTransform.ValueMapToFeatureRowDoFn;
import feast.specs.ImportSpecProto.Field;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.specs.ImportSpecProto.Schema;
import feast.types.FeatureRowProto.FeatureRow;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotEmpty;
import lombok.AllArgsConstructor;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;


/**
 * Transform for processing CSV text jsonfiles and producing FeatureRow messages. CSV jsonfiles with
 * headers are not supported.
 *
 * <p>This transform asserts that the import spec is for only one entity, as all columns must have
 * the same entity. There must be the same number of columns in the CSV jsonfiles as in the import
 * spec.
 *
 * <p>The output is a PCollection of {@link feast.types.FeatureRowProto.FeatureRow FeatureRows},
 * where every feature and entity {@link feast.types.ValueProto.Value Value} in the FeatureRow is a
 * string (set via Value.stringVal).
 */
@AllArgsConstructor
public class CsvFileFeatureSource extends FeatureSource {

  public static final String CSV_FILE_FEATURE_SOURCE_TYPE = "file.csv";

  private final ImportSpec importSpec;

  @Override
  public PCollection<FeatureRow> expand(PInput input) {
    CsvFileFeatureSourceOptions options = OptionsParser
        .parse(importSpec.getSourceOptionsMap(), CsvFileFeatureSourceOptions.class);
    List<String> entities = importSpec.getEntitiesList();
    Preconditions.checkArgument(
        entities.size() == 1, "exactly 1 entity must be set for CSV import");
    Schema schema = importSpec.getSchema();
    String entity = entities.get(0);

    final List<String> fieldNames = Lists.newArrayList();
    final Map<String, Field> fields = Maps.newHashMap();
    for (Field field : schema.getFieldsList()) {
      String displayName = !field.getName().isEmpty() ? field.getName() : field.getFeatureId();
      fieldNames.add(displayName);
      fields.put(displayName, field);
    }

    String path = options.path;
    String entityIdColumn = schema.getEntityIdColumn();
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(entityIdColumn), "entity id column must be set");
    String timestampColumn = schema.getTimestampColumn();
    Preconditions.checkArgument(
        schema.getFieldsList().size() > 0,
        "CSV import needs schema with a least one field specified");

    if (!Strings.isNullOrEmpty(timestampColumn)) {
      Preconditions.checkArgument(fieldNames.contains(timestampColumn),
          String.format("timestampColumn %s, does not match any field", timestampColumn));
    }
    PCollection<String> text = input.getPipeline().apply(TextIO.read().from(path));
    return text.apply(ParseCsvTransform.builder().header(fieldNames).build())
        .apply(new StringToValueMapTransform())
        .apply(ParDo.of(ValueMapToFeatureRowDoFn.of(entity, schema)));
  }

  public static class CsvFileFeatureSourceOptions implements Options {

    @NotEmpty
    public String path;
  }

  @AutoService(FeatureSourceFactory.class)
  public static class Factory implements FeatureSourceFactory {

    @Override
    public String getType() {
      return CSV_FILE_FEATURE_SOURCE_TYPE;
    }

    @Override
    public FeatureSource create(ImportSpec importSpec) {
      checkArgument(importSpec.getType().equals(getType()));
      return new CsvFileFeatureSource(importSpec);
    }
  }
}
