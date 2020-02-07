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
package feast.ingestion.utils;

import java.io.*;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;

public class CompressionUtil {

  public static List<String> decompressAsListOfString(byte[] compressedFeatureSets)
      throws IOException {
    try (ByteArrayInputStream inputStream = new ByteArrayInputStream(compressedFeatureSets);
        BZip2CompressorInputStream bzip2Input = new BZip2CompressorInputStream(inputStream);
        BufferedReader reader = new BufferedReader(new InputStreamReader(bzip2Input)); ) {
      return reader.lines().collect(Collectors.toList());
    }
  }

  public static byte[] compress(String origStr) throws IOException {
    ByteArrayOutputStream compressedStream = new ByteArrayOutputStream();
    try (BZip2CompressorOutputStream bzip2Output =
        new BZip2CompressorOutputStream(compressedStream)) {
      bzip2Output.write(origStr.getBytes());
    }

    return compressedStream.toByteArray();
  }
}
