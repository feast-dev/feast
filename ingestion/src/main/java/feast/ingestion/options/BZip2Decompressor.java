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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;

public class BZip2Decompressor<T> implements OptionDecompressor<T> {

  private final InputStreamConverter<T> inputStreamConverter;

  public BZip2Decompressor(InputStreamConverter<T> inputStreamConverter) {
    this.inputStreamConverter = inputStreamConverter;
  }

  @Override
  public T decompress(byte[] compressed) throws IOException {
    try (ByteArrayInputStream inputStream = new ByteArrayInputStream(compressed);
        BZip2CompressorInputStream bzip2Input = new BZip2CompressorInputStream(inputStream)) {
      return inputStreamConverter.readStream(bzip2Input);
    }
  }
}
