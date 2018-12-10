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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import feast.ingestion.metrics.FeastMetrics;
import feast.ingestion.model.Values;
import feast.ingestion.transform.CsvIO.StringMap;
import feast.ingestion.util.DateUtil;
import feast.specs.ImportSpecProto.Field;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.types.FeatureProto.Feature;
import feast.types.FeatureRowProto.FeatureRow;
import lombok.Builder;
import lombok.NonNull;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class FeatureRowCsvIO {

  public static Read read(ImportSpec importSpec) {
    return Read.builder().importSpec(importSpec).build();
  }

  /**
   * Transform for processing CSV text jsonfiles and producing FeatureRow messages. CSV jsonfiles
   * with headers are not supported.
   *
   * <p>This transform asserts that the import spec is for only one entity, as all columns must have
   * the same entity. There must be the same number of columns in the CSV jsonfiles as in the import
   * spec.
   *
   * <p>The output is a PCollection of {@link feast.types.FeatureRowProto.FeatureRow FeatureRows},
   * where every feature and entity {@link feast.types.ValueProto.Value Value} in the FeatureRow is
   * a string (set via Value.stringVal).
   */
  @Builder
  public static class Read extends FeatureIO.Read {

    @NonNull private final ImportSpec importSpec;

    @Override
    public PCollection<FeatureRow> expand(PInput input) {
      final List<String> fieldNames = Lists.newArrayList();
      final Map<String, Field> fields = Maps.newHashMap();
      for (Field field : importSpec.getSchema().getFieldsList()) {
        String displayName = !field.getName().isEmpty() ? field.getName() : field.getFeatureId();
        fieldNames.add(displayName);
        fields.put(displayName, field);
      }

      String path = importSpec.getOptionsMap().get("path");
      Preconditions.checkArgument(
          !Strings.isNullOrEmpty(path), "path must be set as option in ImportSpec for CSV import");
      Preconditions.checkArgument(
          importSpec.getEntitiesList().size() == 1, "exactly 1 entity must be set for CSV import");
      String entity = importSpec.getEntities(0);
      String entityIdColumn = importSpec.getSchema().getEntityIdColumn();
      Preconditions.checkArgument(
          !Strings.isNullOrEmpty(entityIdColumn), "entity id column must be set");
      String timestampColumn = importSpec.getSchema().getTimestampColumn();
      Preconditions.checkArgument(
          importSpec.getSchema().getFieldsList().size() > 0,
          "CSV import needs schema with a least one field specified");

      PCollection<StringMap> stringMaps = input.getPipeline().apply(CsvIO.read(path, fieldNames));

      return stringMaps.apply(
          ParDo.of(
              new DoFn<StringMap, FeatureRow>() {
                @ProcessElement
                public void processElement(ProcessContext context) {
                  FeatureRow.Builder builder = FeatureRow.newBuilder();
                  try {
                    StringMap stringMap = context.element();
                    builder.setEntityName(entity);


                    for (Entry<String, String> entry : stringMap.entrySet()) {
                      String name = entry.getKey();
                      String value = entry.getValue();
                      Field field = fields.get(name);

                      // A feature can only be one of these things
                      if (entityIdColumn.equals(name)) {
                        builder.setEntityKey(value);
                      } else if (timestampColumn.equals(name)) {
                        builder.setEventTimestamp(DateUtil.toTimestamp(value));
                      } else if (!Strings.isNullOrEmpty(field.getFeatureId())) {
                        String featureId = field.getFeatureId();
                        builder.addFeatures(
                            Feature.newBuilder().setId(featureId).setValue(Values.ofString(value)));
                      }
                      // else silently ignore this column
                    }
                    if (!importSpec
                        .getSchema()
                        .getTimestampValue()
                        .equals(com.google.protobuf.Timestamp.getDefaultInstance())) {
                      // This overrides any column event timestamp.
                      builder.setEventTimestamp(importSpec.getSchema().getTimestampValue());
                    }
                    context.output(builder.build());
                  } catch (Exception e) {
                    FeastMetrics.inc(builder.build(), "input_errors");
                  }
                }
              }));
    }
  }
}
