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

package feast.storage.postgres;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import feast.ingestion.util.DateUtil;
import feast.ingestion.transform.FeatureIO;
import feast.ingestion.transform.SplitFeatures.MultiOutputSplit;
import feast.ingestion.model.Specs;
import com.google.inject.Inject;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.types.GranularityProto.Granularity;
import feast.types.FeatureProto.Feature;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.ValueProto.Value;
import java.io.ByteArrayOutputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.jtwig.JtwigModel;
import org.jtwig.JtwigTemplate;

@Slf4j
public class FeatureRowJdbcIO {



  public static class Write extends FeatureIO.Write {

    private String driverClassName = "org.postgresql.Driver";
    private PostgresOptions jdbcOptions;
    private Specs specs;
    private String storeId;

    private static Map<String, String> driverTemplates = new HashMap<>();

    static {
      driverTemplates.put("org.postgresql.Driver", "upsert_feature_row.postgres.sql.twig");
    }

    @Inject
    public Write(PostgresOptions jdbcOptions, Specs specs, String storeId) {
      this.jdbcOptions = jdbcOptions;
      this.specs = specs;
      this.storeId = storeId;
    }

    public static String getTableName(FeatureSpec spec) {
      String entity = spec.getEntity();
      String granularity = spec.getGranularity().name().toLowerCase();
      return String.format("%s_%s", entity, granularity);
    }

    @Override
    public PDone expand(PCollection<FeatureRowExtended> input) {
      List<FeatureSpec> storeFeatureSpecs = specs.getFeatureSpecByServingStoreId(storeId);
      Set<String> tableNames = new HashSet<>();
      Map<String, Set<FeatureSpec>> featuresByTable = new HashMap<>();

      // Group feature specs by table name
      for (FeatureSpec featureSpec : storeFeatureSpecs) {
        String tableName = getTableName(featureSpec);
        tableNames.add(tableName);
        Set<FeatureSpec> featureSpecs = featuresByTable.getOrDefault(tableName, new HashSet<>());
        featureSpecs.add(featureSpec);
        featuresByTable.put(tableName, featureSpecs);
      }

      MultiOutputSplit<String> splitter =
          new MultiOutputSplit<>(Write::getTableName, tableNames, specs);
      PCollectionTuple splitByTable = input.apply(splitter);

      for (String tableName : tableNames) {
        log.info("Initialising write transform for table " + tableName);
        TupleTag<FeatureRowExtended> tag = splitter.getStrategy().getTag(tableName);
        writeTable(splitByTable.get(tag), Lists.newArrayList(featuresByTable.get(tableName)));
      }
      return PDone.in(input.getPipeline());
    }

    private PDone writeTable(
        PCollection<FeatureRowExtended> input, List<FeatureSpec> featureSpecs) {
      String tableName =
          Strings.nullToEmpty(jdbcOptions.prefix) + getTableName(featureSpecs.get(0));

      // We create map of positions so that the prepared statement knows what order to set feature
      // columns
      final Map<String, Integer> positions = new HashMap<>();
      for (int i = 0; i < featureSpecs.size(); i++) {
        positions.put(featureSpecs.get(i).getId(), i);
      }

      JtwigTemplate template =
          JtwigTemplate.classpathTemplate("templates/" + driverTemplates.get(driverClassName));
      JtwigModel model =
          JtwigModel.newModel()
              .with("tableName", tableName)
              .with(
                  "featureNames",
                  featureSpecs.stream().map(FeatureSpec::getName).collect(Collectors.toList()));

      ByteArrayOutputStream outs = new ByteArrayOutputStream();
      template.render(model, outs);
      String statement = new String(outs.toByteArray(), Charsets.UTF_8);

      return input.apply(
          JdbcIO.<FeatureRowExtended>write()
              .withDataSourceConfiguration(
                  JdbcIO.DataSourceConfiguration.create(driverClassName, jdbcOptions.url))
              .withStatement(statement)
              .withPreparedStatementSetter(
                  new JdbcIO.PreparedStatementSetter<FeatureRowExtended>() {
                    public void setParameters(
                        FeatureRowExtended extendedRow, PreparedStatement query)
                        throws SQLException {
                      // We assume all features have the same granularity
                      // (this should be enforced by validation)

                      FeatureRow row = extendedRow.getRow();
                      Granularity.Enum granularity = row.getGranularity();

                      query.setString(1, row.getEntityKey());
                      query.setTimestamp(2, DateUtil.toSqlTimestamp(row.getEventTimestamp()));

                      int featuresOffset = 3;
                      for (Feature feature : row.getFeaturesList()) {
                        Integer featurePosition = positions.get(feature.getId());
                        if (featurePosition == null) {
                          continue;
                        }
                        setPreparedStatementValue(
                            query, featuresOffset + featurePosition, feature.getValue());
                      }
                    }
                  }));
    }

    private void setPreparedStatementValue(PreparedStatement query, int index, Value value)
        throws SQLException {
      switch (value.getValCase()) {
        case STRINGVAL:
          query.setString(index, value.getStringVal());
          break;
        case TIMESTAMPVAL:
          query.setTimestamp(index, DateUtil.toSqlTimestamp(value.getTimestampVal()));
          break;
        case DOUBLEVAL:
          query.setDouble(index, value.getDoubleVal());
          break;
        case BOOLVAL:
          query.setBoolean(index, value.getBoolVal());
          break;
        case BYTESVAL:
          query.setBytes(index, value.getBytesVal().toByteArray());
          break;
        case FLOATVAL:
          query.setFloat(index, value.getFloatVal());
          break;
        case INT64VAL:
          query.setLong(index, value.getInt64Val());
          break;
        case INT32VAL:
          query.setInt(index, value.getInt32Val());
          break;
        default:
          throw new SQLException("Unhandled type " + value.getValCase().name());
      }
    }
  }
}
