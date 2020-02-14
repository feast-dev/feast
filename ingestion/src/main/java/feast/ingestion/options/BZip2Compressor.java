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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;

public class BZip2Compressor<T> implements OptionCompressor<T> {

  private final OptionByteConverter<T> byteConverter;

  public BZip2Compressor(OptionByteConverter<T> byteConverter) {
    this.byteConverter = byteConverter;
  }
  /**
   * Compress pipeline option using BZip2
   *
   * @param option Pipeline option value
   * @return BZip2 compressed option value
   * @throws IOException
   */
  @Override
  public byte[] compress(T option) throws IOException {
    ByteArrayOutputStream compressedStream = new ByteArrayOutputStream();
    try (BZip2CompressorOutputStream bzip2Output =
        new BZip2CompressorOutputStream(compressedStream)) {
      bzip2Output.write(byteConverter.toByte(option));
    }

    return compressedStream.toByteArray();
  }
}
