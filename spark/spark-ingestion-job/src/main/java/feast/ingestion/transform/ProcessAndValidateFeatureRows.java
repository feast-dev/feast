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
package feast.ingestion.transform;

import feast.ingestion.transform.fn.ProcessFeatureRowDoFn;
import feast.ingestion.transform.fn.ValidateFeatureRowDoFn;
import feast.ingestion.values.FeatureSet;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.spark.ingestion.RowWithValidationResult;
import java.util.HashMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

public class ProcessAndValidateFeatureRows {

  private final String defaultFeastProject;

  public ProcessAndValidateFeatureRows(String defaultFeastProject) {
    this.defaultFeastProject = defaultFeastProject;
  }

  public Dataset<RowWithValidationResult> processDataset(
      Dataset<Row> input, HashMap<String, FeatureSet> featureSets) {
    ValidateFeatureRowDoFn validFeat = new ValidateFeatureRowDoFn(featureSets);

    ProcessFeatureRowDoFn procFeat = new ProcessFeatureRowDoFn(defaultFeastProject);

    Dataset<RowWithValidationResult> rowsWithValidationResult =
        input
            .select("value")
            .map(
                r -> {
                  FeatureRow featureRow = FeatureRow.parseFrom((byte[]) r.getAs(0));
                  FeatureRow el = procFeat.processElement(featureRow);
                  return validFeat.validateElement(el);
                },
                Encoders.kryo(RowWithValidationResult.class));
    return rowsWithValidationResult;
  }
}
