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
package feast.core.logging;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import feast.common.logging.entry.AuditLogEntryKind;
import feast.core.it.BaseIT;
import feast.core.it.DataGenerator;
import feast.proto.core.CoreServiceGrpc;
import feast.proto.core.CoreServiceGrpc.CoreServiceBlockingStub;
import feast.proto.core.CoreServiceProto.GetFeastCoreVersionRequest;
import feast.proto.core.CoreServiceProto.ListFeatureSetsRequest;
import feast.proto.core.CoreServiceProto.UpdateStoreRequest;
import feast.proto.core.CoreServiceProto.UpdateStoreResponse;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(
    properties = {
      "feast.logging.audit.enabled=true",
      "feast.logging.audit.messageLoggingEnabled=true",
    })
public class CoreLoggingIT extends BaseIT {
  private static TestLogAppender testAuditLogAppender;
  private static CoreServiceBlockingStub coreService;

  @BeforeAll
  public static void globalSetUp(@Value("${grpc.server.port}") int coreGrpcPort)
      throws InterruptedException, ExecutionException {
    LoggerContext logContext = (LoggerContext) LogManager.getContext(false);
    testAuditLogAppender = logContext.getConfiguration().getAppender("TestAuditLogAppender");

    // Connect to core.
    Channel channel =
        ManagedChannelBuilder.forAddress("localhost", coreGrpcPort).usePlaintext().build();
    coreService = CoreServiceGrpc.newBlockingStub(channel);

    // Preflight a request to core service stub to verify connection
    coreService.getFeastCoreVersion(GetFeastCoreVersionRequest.getDefaultInstance());
  }

  /**
   * Test Message Audit Logging. Produce artifical load on Feast core and checking the produced
   * message logs.
   */
  @Test
  public void shouldProduceMessageAuditLogs() throws InterruptedException {
    testAuditLogAppender.getLogs().clear();
    // Generate artifical load on feast core.
    UpdateStoreRequest request =
        UpdateStoreRequest.newBuilder().setStore(DataGenerator.getDefaultStore()).build();
    UpdateStoreResponse receivedResponse = null;
    final int LOAD_SIZE = 200;
    final int ASYNC_BURST_SIZE = 5;
    for (int i = 0; i < LOAD_SIZE; i++) {
      receivedResponse = coreService.updateStore(request);
    }
    final UpdateStoreResponse response = receivedResponse;

    // Wait required to ensure audit logs are flushed into test audit log appender
    Thread.sleep(1000);
    // Check message audit logs are produced for each audit log.
    JsonFormat.Parser protoJSONParser = JsonFormat.parser();
    List<JsonObject> logJsonObjects = parseMessageJsonLogObjects(testAuditLogAppender.getLogs());

    logJsonObjects.forEach(
        logObj -> {
          // Extract recorded request/response from message logs
          try {
            String requestJson = logObj.getAsJsonObject("request").toString();
            UpdateStoreRequest.Builder gotRequest = UpdateStoreRequest.newBuilder();
            protoJSONParser.merge(requestJson, gotRequest);

            String responseJson = logObj.getAsJsonObject("response").toString();
            UpdateStoreResponse.Builder gotResponse = UpdateStoreResponse.newBuilder();
            protoJSONParser.merge(responseJson, gotResponse);

            // Check that request/response are returned correctlyf
            assertThat(gotRequest.build(), equalTo(request));
            assertThat(gotResponse.build(), equalTo(response));
          } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
          }
        });
  }

  /** Check that message audit logs are produced when server encounters an error */
  @Test
  public void shouldProduceMessageAuditLogsOnError() throws InterruptedException {
    testAuditLogAppender.getLogs().clear();
    // send a bad request which should cause Core to error
    ListFeatureSetsRequest request =
        ListFeatureSetsRequest.newBuilder()
            .setFilter(
                ListFeatureSetsRequest.Filter.newBuilder()
                    .setProject("*")
                    .setFeatureSetName("nop")
                    .build())
            .build();

    boolean hasExpectedException = false;
    Code statusCode = null;
    try {
      coreService.listFeatureSets(request);
    } catch (StatusRuntimeException e) {
      hasExpectedException = true;
      statusCode = e.getStatus().getCode();
    }
    assertTrue(hasExpectedException);

    // Wait required to ensure audit logs are flushed into test audit log appender
    Thread.sleep(1000);
    List<JsonObject> logJsonObjects = parseMessageJsonLogObjects(testAuditLogAppender.getLogs());
    assertEquals(logJsonObjects.size(), 1);
    JsonObject logJsonObject = logJsonObjects.get(0);
    assertEquals(logJsonObject.get("statusCode").getAsString(), statusCode.toString());
  }

  /** Filter and Parse out Message Audit Logs from the given logsStrings */
  private List<JsonObject> parseMessageJsonLogObjects(List<String> logsStrings) {
    JsonParser jsonParser = new JsonParser();
    // copy to prevent concurrent modification.
    List<String> logsStringsCopy = new ArrayList<String>(logsStrings);
    return logsStringsCopy.stream()
        .map(logJSON -> jsonParser.parse(logJSON).getAsJsonObject())
        // Filter to only include message audit logs
        .filter(
            logObj ->
                logObj
                    .getAsJsonPrimitive("kind")
                    .getAsString()
                    .equals(AuditLogEntryKind.MESSAGE.toString()))
        .collect(Collectors.toList());
  }
}
