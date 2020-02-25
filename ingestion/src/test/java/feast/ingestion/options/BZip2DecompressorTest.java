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
package feast.ingestion.options;

import static org.junit.Assert.*;

import java.io.*;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.junit.Test;

public class BZip2DecompressorTest {

  @Test
  public void shouldDecompressBZip2Stream() throws IOException {
    BZip2Decompressor<String> decompressor =
        new BZip2Decompressor<>(
            inputStream -> {
              BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
              String output = reader.readLine();
              reader.close();
              return output;
            });

    String originalString = "abc";
    ByteArrayOutputStream compressedStream = new ByteArrayOutputStream();
    try (BZip2CompressorOutputStream bzip2Output =
        new BZip2CompressorOutputStream(compressedStream)) {
      bzip2Output.write(originalString.getBytes());
    }

    String decompressedString = decompressor.decompress(compressedStream.toByteArray());
    assertEquals(originalString, decompressedString);
  }
}
