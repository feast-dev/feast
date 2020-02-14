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
package feast.core.job.option;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import feast.core.FeatureSetProto;
import feast.ingestion.options.OptionByteConverter;
import java.util.ArrayList;
import java.util.List;

public class FeatureSetJsonByteConverter
    implements OptionByteConverter<List<FeatureSetProto.FeatureSet>> {

  /**
   * Convert list of feature sets to json strings joined by new line, represented as byte arrays
   *
   * @param featureSets List of feature set protobufs
   * @return Byte array representation of the json strings
   * @throws InvalidProtocolBufferException
   */
  @Override
  public byte[] toByte(List<FeatureSetProto.FeatureSet> featureSets)
      throws InvalidProtocolBufferException {
    JsonFormat.Printer printer =
        JsonFormat.printer().omittingInsignificantWhitespace().printingEnumsAsInts();
    List<String> featureSetsJson = new ArrayList<>();
    for (FeatureSetProto.FeatureSet featureSet : featureSets) {
      featureSetsJson.add(printer.print(featureSet.getSpec()));
    }
    return String.join("\n", featureSetsJson).getBytes();
  }
}
