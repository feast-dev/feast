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
package feast.serving.test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse.FieldValues;
import feast.types.ValueProto.Value;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;

@SuppressWarnings("WeakerAccess")
public class TestUtil {

  public static class LocalCassandra {

    public static void start() throws InterruptedException, IOException, TTransportException {
      EmbeddedCassandraServerHelper.startEmbeddedCassandra();
    }

    public static void createKeyspaceAndTable() {
      new ClassPathCQLDataSet("embedded-store/LoadCassandra.cql", true, true)
          .getCQLStatements()
          .forEach(s -> LocalCassandra.getSession().execute(s));
    }

    public static String getHost() {
      return EmbeddedCassandraServerHelper.getHost();
    }

    public static int getPort() {
      return EmbeddedCassandraServerHelper.getNativeTransportPort();
    }

    public static Cluster getCluster() {
      return EmbeddedCassandraServerHelper.getCluster();
    }

    public static Session getSession() {
      return EmbeddedCassandraServerHelper.getSession();
    }

    public static void stop() {
      EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }
  }

  public static List<Map<String, Value>> responseToMapList(GetOnlineFeaturesResponse response) {
    return response.getFieldValuesList().stream()
        .map(FieldValues::getFieldsMap)
        .collect(Collectors.toList());
  }

  public static Value intValue(int val) {
    return Value.newBuilder().setInt64Val(val).build();
  }

  public static Value strValue(String val) {
    return Value.newBuilder().setStringVal(val).build();
  }
}
