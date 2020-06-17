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
package com.google.cloud.bigquery;

import java.io.IOException;
import java.io.ObjectInputStream;

public class FakeTable extends Table {
  FakeTable(BigQuery bigquery, BuilderImpl infoBuilder) {
    super(bigquery, infoBuilder);
  }

  public static FakeTable create(BigQuery bigQuery, TableId tableId, TableDefinition definition) {
    BuilderImpl builder = new BuilderImpl();
    builder.setDefinition(definition);
    builder.setTableId(tableId);
    return new FakeTable(bigQuery, builder);
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
  }
}
