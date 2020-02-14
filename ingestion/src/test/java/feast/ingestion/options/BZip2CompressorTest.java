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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.junit.Assert;
import org.junit.Test;

public class BZip2CompressorTest {

  @Test
  public void shouldHavBZip2CompatibleOutput() throws IOException {
    BZip2Compressor<String> compressor = new BZip2Compressor<>(String::getBytes);
    String origString = "somestring";
    try (ByteArrayInputStream inputStream =
            new ByteArrayInputStream(compressor.compress(origString));
        BZip2CompressorInputStream bzip2Input = new BZip2CompressorInputStream(inputStream);
        BufferedReader reader = new BufferedReader(new InputStreamReader(bzip2Input))) {
      Assert.assertEquals(origString, reader.readLine());
    }
  }
}
