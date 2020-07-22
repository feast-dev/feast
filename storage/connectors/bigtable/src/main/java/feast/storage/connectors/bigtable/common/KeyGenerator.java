/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
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
package feast.storage.connectors.bigtable.common;

import feast.proto.storage.BigtableProto.BigtableKey;
import feast.proto.types.FieldProto.Field;
import org.apache.beam.sdk.values.*;

public class KeyGenerator {

  /**
   * private FailedElement toFailedElement( FeatureRow featureRow, Exception exception, String
   * jobName) { return FailedElement.newBuilder() .setJobName(jobName)
   * .setTransformName("BigtableCustomIO") .setPayload(featureRow.toString())
   * .setErrorMessage(exception.getMessage())
   * .setStackTrace(ExceptionUtils.getStackTrace(exception)) .build(); } *
   */
  public static String getKey(BigtableKey btk, String project, String name) {
    StringBuilder bigtableKey = new StringBuilder();
    for (Field field : btk.getEntitiesList()) {
      bigtableKey.append(field.getValue().getStringVal());
    }
    bigtableKey.append("#");
    for (Field field : btk.getEntitiesList()) {
      bigtableKey.append(field.getName());
      bigtableKey.append("#");
    }
    bigtableKey.append(project);
    bigtableKey.append("#");
    bigtableKey.append(name);
    return bigtableKey.toString();
  }
}
