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

import java.io.IOException;

public interface OptionByteConverter<T> {

  /**
   * Used in conjunction with {@link OptionCompressor} to compress the pipeline option
   *
   * @param option Pipeline option value
   * @return byte representation of the pipeline option value, without compression.
   */
  byte[] toByte(T option) throws IOException;
}
