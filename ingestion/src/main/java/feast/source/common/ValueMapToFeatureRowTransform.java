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

package feast.source.common;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import feast.ingestion.metrics.FeastMetrics;
import feast.ingestion.model.Values;
import feast.specs.ImportSpecProto.Field;
import feast.specs.ImportSpecProto.Schema;
import feast.types.FeatureProto.Feature;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.ValueProto.Value;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class ValueMapToFeatureRowTransform extends
    PTransform<PCollection<Map<String, Value>>, PCollection<FeatureRow>> {

  private String entity;
  private Schema schema;

  public ValueMapToFeatureRowTransform(String entity, Schema schema) {
    this.entity = entity;
    this.schema = schema;
  }

  @Override
  public PCollection<FeatureRow> expand(PCollection<Map<String, Value>> input) {
    return input.apply(ParDo.of(ValueMapToFeatureRowDoFn.of(entity, schema)))
        .setCoder(ProtoCoder.of(FeatureRow.class));
  }

  public static class ValueMapToFeatureRowDoFn extends
      DoFn<Map<String, Value>, FeatureRow> {

    private String entityIdColumn;
    private String timestampColumn;
    private com.google.protobuf.Timestamp timestampValue;
    private String entity;
    private Map<String, Field> fields;

    private ValueMapToFeatureRowDoFn(String entity, Map<String, Field> fields,
        String entityIdColumn,
        String timestampColumn,
        com.google.protobuf.Timestamp timestampValue) {
      this.entity = entity;
      this.fields = fields;
      this.entityIdColumn = entityIdColumn;
      this.timestampColumn = timestampColumn;
      this.timestampValue = timestampValue;
    }

    public static ValueMapToFeatureRowDoFn of(String entity, Schema schema) {
      final Map<String, Field> fields = Maps.newHashMap();
      for (Field field : schema.getFieldsList()) {
        String displayName = !field.getName().isEmpty() ? field.getName() : field.getFeatureId();
        fields.put(displayName, field);
      }
      return new ValueMapToFeatureRowDoFn(entity, fields, schema.getEntityIdColumn(),
          schema.getTimestampColumn(), schema.getTimestampValue());
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      FeatureRow.Builder builder = FeatureRow.newBuilder();
      try {
        Map<String, Value> valueMap = context.element();
        builder.setEntityName(entity);
        for (Entry<String, Value> entry : valueMap.entrySet()) {
          String name = entry.getKey();
          Value value = entry.getValue();
          Field field = fields.get(name);

          // A feature can only be one of these things
          if (entityIdColumn.equals(name)) {
            builder.setEntityKey(Values.asString(value).getStringVal());
          } else if (timestampColumn.equals(name)) {
            builder.setEventTimestamp(Values.asTimestamp(value).getTimestampVal());
          } else if (!Strings.isNullOrEmpty(field.getFeatureId())) {
            String featureId = field.getFeatureId();
            builder.addFeatures(
                Feature.newBuilder().setId(featureId).setValue(value));
          }
          // else silently ignore this column
        }
        if (!timestampValue
            .equals(com.google.protobuf.Timestamp.getDefaultInstance())) {
          // This overrides any column event timestamp.
          builder.setEventTimestamp(timestampValue);
        }
        context.output(builder.build());
      } catch (Exception e) {
        FeastMetrics.inc(builder.build(), "input_errors");
      }
    }
  }
}