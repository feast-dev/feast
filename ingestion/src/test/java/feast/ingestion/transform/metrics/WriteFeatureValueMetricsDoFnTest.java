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
package feast.ingestion.transform.metrics;

import static org.junit.Assert.fail;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import feast.test.TestUtil.DummyStatsDServer;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FeatureRowProto.FeatureRow.Builder;
import feast.types.FieldProto.Field;
import feast.types.ValueProto.BoolList;
import feast.types.ValueProto.BytesList;
import feast.types.ValueProto.DoubleList;
import feast.types.ValueProto.FloatList;
import feast.types.ValueProto.Int32List;
import feast.types.ValueProto.Int64List;
import feast.types.ValueProto.StringList;
import feast.types.ValueProto.Value;
import java.io.BufferedReader;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.Rule;
import org.junit.Test;

public class WriteFeatureValueMetricsDoFnTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  private static final int STATSD_SERVER_PORT = 17254;
  private final DummyStatsDServer statsDServer = new DummyStatsDServer(STATSD_SERVER_PORT);

  @Test
  public void shouldSendCorrectStatsDMetrics() throws IOException, InterruptedException {
    PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
    pipelineOptions.setJobName("job");

    Map<String, Iterable<FeatureRow>> input =
        readTestInput("feast/ingestion/transform/WriteFeatureValueMetricsDoFnTest.input");
    List<String> expectedLines =
        readTestOutput("feast/ingestion/transform/WriteFeatureValueMetricsDoFnTest.output");

    pipeline
        .apply(Create.of(input))
        .apply(
            ParDo.of(
                WriteFeatureValueMetricsDoFn.newBuilder()
                    .setStatsdHost("localhost")
                    .setStatsdPort(STATSD_SERVER_PORT)
                    .setStoreName("store")
                    .build()));
    pipeline.run(pipelineOptions).waitUntilFinish();
    // Wait until StatsD has finished processed all messages, 3 sec is a reasonable duration
    // based on empirical testing.
    Thread.sleep(3000);

    List<String> actualLines = statsDServer.messagesReceived();
    for (String expected : expectedLines) {
      boolean matched = false;
      for (String actual : actualLines) {
        if (actual.equals(expected)) {
          matched = true;
          break;
        }
      }
      if (!matched) {
        System.out.println("Print actual metrics output for debugging:");
        for (String line : actualLines) {
          System.out.println(line);
        }
        fail(String.format("Expected StatsD metric not found:\n%s", expected));
      }
    }
    statsDServer.stop();
  }

  // Test utility method to read expected StatsD metrics output from a text file.
  @SuppressWarnings("SameParameterValue")
  public static List<String> readTestOutput(String path) throws IOException {
    URL url = Thread.currentThread().getContextClassLoader().getResource(path);
    if (url == null) {
      throw new IllegalArgumentException(
          "cannot read test data, path contains null url. Path: " + path);
    }
    List<String> lines = new ArrayList<>();
    try (BufferedReader reader = Files.newBufferedReader(Paths.get(url.getPath()))) {
      String line = reader.readLine();
      while (line != null) {
        if (line.trim().length() > 1) {
          lines.add(line);
        }
        line = reader.readLine();
      }
    }
    return lines;
  }

  public static Map<String, Iterable<FeatureRow>> readTestInput(String path) throws IOException {
    return readTestInput(path, null);
  }

  // Test utility method to create test feature row data from a text file.
  // If tsOverride is not null, all the feature row will have the same timestamp "tsOverride".
  // Else if there exist a "timestamp" column with RFC3339 format, the feature row will be assigned
  // that timestamp.
  // Else no timestamp will be assigned (the feature row will have the default proto Timestamp
  // object).
  @SuppressWarnings("SameParameterValue")
  public static Map<String, Iterable<FeatureRow>> readTestInput(String path, Timestamp tsOverride)
      throws IOException {
    Map<String, List<FeatureRow>> data = new HashMap<>();
    URL url = Thread.currentThread().getContextClassLoader().getResource(path);
    if (url == null) {
      throw new IllegalArgumentException(
          "cannot read test data, path contains null url. Path: " + path);
    }
    List<String> lines = new ArrayList<>();
    try (BufferedReader reader = Files.newBufferedReader(Paths.get(url.getPath()))) {
      String line = reader.readLine();
      while (line != null) {
        lines.add(line);
        line = reader.readLine();
      }
    }
    List<String> colNames = new ArrayList<>();
    for (String line : lines) {
      if (line.trim().length() < 1) {
        continue;
      }
      String[] splits = line.split(",");
      colNames.addAll(Arrays.asList(splits));

      if (line.startsWith("featuresetref")) {
        // Header line
        colNames.addAll(Arrays.asList(splits).subList(1, splits.length));
        continue;
      }

      Builder featureRowBuilder = FeatureRow.newBuilder();
      for (int i = 0; i < splits.length; i++) {
        String colVal = splits[i].trim();
        if (i == 0) {
          featureRowBuilder.setFeatureSet(colVal);
          continue;
        }
        String colName = colNames.get(i);
        if (colName.equals("timestamp")) {
          Instant instant = Instant.parse(colVal);
          featureRowBuilder.setEventTimestamp(
              Timestamps.fromNanos(instant.getEpochSecond() * 1_000_000_000 + instant.getNano()));
          continue;
        }

        Field.Builder fieldBuilder = Field.newBuilder().setName(colName);
        if (!colVal.isEmpty()) {
          switch (colName) {
            case "int32":
              fieldBuilder.setValue(Value.newBuilder().setInt32Val((Integer.parseInt(colVal))));
              break;
            case "int64":
              fieldBuilder.setValue(Value.newBuilder().setInt64Val((Long.parseLong(colVal))));
              break;
            case "double":
              fieldBuilder.setValue(Value.newBuilder().setDoubleVal((Double.parseDouble(colVal))));
              break;
            case "float":
              fieldBuilder.setValue(Value.newBuilder().setFloatVal((Float.parseFloat(colVal))));
              break;
            case "bool":
              fieldBuilder.setValue(Value.newBuilder().setBoolVal((Boolean.parseBoolean(colVal))));
              break;
            case "int32list":
              List<Integer> int32List = new ArrayList<>();
              for (String val : colVal.split("\\|")) {
                int32List.add(Integer.parseInt(val));
              }
              fieldBuilder.setValue(
                  Value.newBuilder().setInt32ListVal(Int32List.newBuilder().addAllVal(int32List)));
              break;
            case "int64list":
              List<Long> int64list = new ArrayList<>();
              for (String val : colVal.split("\\|")) {
                int64list.add(Long.parseLong(val));
              }
              fieldBuilder.setValue(
                  Value.newBuilder().setInt64ListVal(Int64List.newBuilder().addAllVal(int64list)));
              break;
            case "doublelist":
              List<Double> doubleList = new ArrayList<>();
              for (String val : colVal.split("\\|")) {
                doubleList.add(Double.parseDouble(val));
              }
              fieldBuilder.setValue(
                  Value.newBuilder()
                      .setDoubleListVal(DoubleList.newBuilder().addAllVal(doubleList)));
              break;
            case "floatlist":
              List<Float> floatList = new ArrayList<>();
              for (String val : colVal.split("\\|")) {
                floatList.add(Float.parseFloat(val));
              }
              fieldBuilder.setValue(
                  Value.newBuilder().setFloatListVal(FloatList.newBuilder().addAllVal(floatList)));
              break;
            case "boollist":
              List<Boolean> boolList = new ArrayList<>();
              for (String val : colVal.split("\\|")) {
                boolList.add(Boolean.parseBoolean(val));
              }
              fieldBuilder.setValue(
                  Value.newBuilder().setBoolListVal(BoolList.newBuilder().addAllVal(boolList)));
              break;
            case "bytes":
              fieldBuilder.setValue(
                  Value.newBuilder().setBytesVal(ByteString.copyFromUtf8("Dummy")));
              break;
            case "byteslist":
              fieldBuilder.setValue(
                  Value.newBuilder().setBytesListVal(BytesList.getDefaultInstance()));
              break;
            case "string":
              fieldBuilder.setValue(Value.newBuilder().setStringVal("Dummy"));
              break;
            case "stringlist":
              fieldBuilder.setValue(
                  Value.newBuilder().setStringListVal(StringList.getDefaultInstance()));
              break;
          }
        }
        featureRowBuilder.addFields(fieldBuilder);
      }

      if (!data.containsKey(featureRowBuilder.getFeatureSet())) {
        data.put(featureRowBuilder.getFeatureSet(), new ArrayList<>());
      }
      List<FeatureRow> featureRowsByFeatureSetRef = data.get(featureRowBuilder.getFeatureSet());
      if (tsOverride != null) {
        featureRowBuilder.setEventTimestamp(tsOverride);
      }
      featureRowsByFeatureSetRef.add(featureRowBuilder.build());
    }

    // Convert List<FeatureRow> to Iterable<FeatureRow> to match the function signature in
    // WriteFeatureValueMetricsDoFn
    Map<String, Iterable<FeatureRow>> dataWithIterable = new HashMap<>();
    for (Entry<String, List<FeatureRow>> entrySet : data.entrySet()) {
      String key = entrySet.getKey();
      Iterable<FeatureRow> value = entrySet.getValue();
      dataWithIterable.put(key, value);
    }
    return dataWithIterable;
  }
}
