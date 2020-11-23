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

import com.google.common.collect.Streams;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import feast.common.it.BaseIT;
import feast.common.it.DataGenerator;
import feast.common.logging.entry.AuditLogEntryKind;
import feast.proto.core.CoreServiceGrpc;
import feast.proto.core.CoreServiceGrpc.CoreServiceBlockingStub;
import feast.proto.core.CoreServiceGrpc.CoreServiceFutureStub;
import feast.proto.core.CoreServiceProto.GetFeastCoreVersionRequest;
import feast.proto.core.CoreServiceProto.ListFeatureTablesRequest;
import feast.proto.core.CoreServiceProto.ListStoresRequest;
import feast.proto.core.CoreServiceProto.ListStoresResponse;
import feast.proto.core.CoreServiceProto.UpdateStoreRequest;
import feast.proto.core.CoreServiceProto.UpdateStoreResponse;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(
    properties = {
      "feast.logging.audit.enabled=true",
      "feast.logging.audit.messageLogging.enabled=true",
      "feast.logging.audit.messageLogging.destination=console"
    })
public class CoreLoggingIT extends BaseIT {
  private static TestLogAppender testAuditLogAppender;
  private static CoreServiceBlockingStub coreService;
  private static CoreServiceFutureStub asyncCoreService;

  @BeforeAll
  public static void globalSetUp(@Value("${grpc.server.port}") int coreGrpcPort)
      throws InterruptedException, ExecutionException {
    LoggerContext logContext = (LoggerContext) LogManager.getContext(false);
    // NOTE: As log appender state is shared across tests use a different method
    // for each test and filter by method name to ensure that you only get logs
    // for a specific test.
    testAuditLogAppender = logContext.getConfiguration().getAppender("TestAuditLogAppender");

    // Connect to core service.
    Channel channel =
        ManagedChannelBuilder.forAddress("localhost", coreGrpcPort).usePlaintext().build();
    coreService = CoreServiceGrpc.newBlockingStub(channel);
    asyncCoreService = CoreServiceGrpc.newFutureStub(channel);

    // Preflight a request to core service stubs to verify connection
    coreService.getFeastCoreVersion(GetFeastCoreVersionRequest.getDefaultInstance());
    asyncCoreService.getFeastCoreVersion(GetFeastCoreVersionRequest.getDefaultInstance()).get();
  }

  /** Check that messsage audit log are produced on service call */
  @Test
  public void shouldProduceMessageAuditLogsOnCall()
      throws InterruptedException, InvalidProtocolBufferException {
    // Generate artifical load on feast core.
    UpdateStoreRequest request =
        UpdateStoreRequest.newBuilder().setStore(DataGenerator.getDefaultStore()).build();
    UpdateStoreResponse response = coreService.updateStore(request);

    // Wait required to ensure audit logs are flushed into test audit log appender
    Thread.sleep(1000);
    // Check message audit logs are produced for each audit log.
    JsonFormat.Parser protoJSONParser = JsonFormat.parser();
    // Pull message audit logs logs from test log appender
    List<JsonObject> logJsonObjects =
        parseMessageJsonLogObjects(testAuditLogAppender.getLogs(), "UpdateStore");
    assertEquals(1, logJsonObjects.size());
    JsonObject logObj = logJsonObjects.get(0);

    // Extract & Check that request/response are returned correctly
    String requestJson = logObj.getAsJsonObject("request").toString();
    UpdateStoreRequest.Builder gotRequest = UpdateStoreRequest.newBuilder();
    protoJSONParser.merge(requestJson, gotRequest);

    String responseJson = logObj.getAsJsonObject("response").toString();
    UpdateStoreResponse.Builder gotResponse = UpdateStoreResponse.newBuilder();
    protoJSONParser.merge(responseJson, gotResponse);

    assertThat(gotRequest.build(), equalTo(request));
    assertThat(gotResponse.build(), equalTo(response));
  }

  /** Check that message audit logs are produced when server encounters an error */
  @Test
  public void shouldProduceMessageAuditLogsOnError() throws InterruptedException {
    // Send a bad request which should cause Core to error
    ListFeatureTablesRequest request =
        ListFeatureTablesRequest.newBuilder()
            .setFilter(ListFeatureTablesRequest.Filter.newBuilder().setProject("*").build())
            .build();

    boolean hasExpectedException = false;
    Code statusCode = null;
    try {
      coreService.listFeatureTables(request);
    } catch (StatusRuntimeException e) {
      hasExpectedException = true;
      statusCode = e.getStatus().getCode();
    }
    assertTrue(hasExpectedException);

    // Wait required to ensure audit logs are flushed into test audit log appender
    Thread.sleep(1000);
    // Pull message audit logs logs from test log appender
    List<JsonObject> logJsonObjects =
        parseMessageJsonLogObjects(testAuditLogAppender.getLogs(), "ListFeatureTables");

    assertEquals(1, logJsonObjects.size());
    JsonObject logJsonObject = logJsonObjects.get(0);
    // Check correct status code is tracked on error.
    assertEquals(logJsonObject.get("statusCode").getAsString(), statusCode.toString());
  }

  /** Check that expected message audit logs are produced when under load. */
  @Test
  public void shouldProduceExpectedAuditLogsUnderLoad()
      throws InterruptedException, ExecutionException {
    // Generate artifical requests on core to simulate load.
    int LOAD_SIZE = 40; // Total number of requests to send.
    int BURST_SIZE = 5; // Number of requests to send at once.

    ListStoresRequest request = ListStoresRequest.getDefaultInstance();
    List<ListStoresResponse> responses = new LinkedList<>();
    for (int i = 0; i < LOAD_SIZE; i += 5) {
      List<ListenableFuture<ListStoresResponse>> futures = new LinkedList<>();
      for (int j = 0; j < BURST_SIZE; j++) {
        futures.add(asyncCoreService.listStores(request));
      }

      responses.addAll(Futures.allAsList(futures).get());
    }
    // Wait required to ensure audit logs are flushed into test audit log appender
    Thread.sleep(1000);

    // Pull message audit logs from test log appender
    List<JsonObject> logJsonObjects =
        parseMessageJsonLogObjects(testAuditLogAppender.getLogs(), "ListStores");
    assertEquals(responses.size(), logJsonObjects.size());

    // Extract & Check that request/response are returned correctly
    JsonFormat.Parser protoJSONParser = JsonFormat.parser();
    Streams.zip(
            responses.stream(),
            logJsonObjects.stream(),
            (response, logObj) -> Pair.of(response, logObj))
        .forEach(
            responseLogJsonPair -> {
              ListStoresResponse response = responseLogJsonPair.getLeft();
              JsonObject logObj = responseLogJsonPair.getRight();

              ListStoresRequest.Builder gotRequest = null;
              ListStoresResponse.Builder gotResponse = null;
              try {
                String requestJson = logObj.getAsJsonObject("request").toString();
                gotRequest = ListStoresRequest.newBuilder();
                protoJSONParser.merge(requestJson, gotRequest);

                String responseJson = logObj.getAsJsonObject("response").toString();
                gotResponse = ListStoresResponse.newBuilder();
                protoJSONParser.merge(responseJson, gotResponse);
              } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
              }

              assertThat(gotRequest.build(), equalTo(request));
              assertThat(gotResponse.build(), equalTo(response));
            });
  }

  /**
   * Filter and Parse out Message Audit Logs from the given logsStrings for the given method name
   */
  private List<JsonObject> parseMessageJsonLogObjects(List<String> logsStrings, String methodName) {
    JsonParser jsonParser = new JsonParser();
    // copy to prevent concurrent modification.
    return logsStrings.stream()
        .map(logJSON -> jsonParser.parse(logJSON).getAsJsonObject())
        // Filter to only include message audit logs
        .filter(
            logObj ->
                logObj
                        .getAsJsonPrimitive("kind")
                        .getAsString()
                        .equals(AuditLogEntryKind.MESSAGE.toString())
                    // filter by method name to ensure logs from other tests do not interfere with
                    // test
                    && logObj.get("method").getAsString().equals(methodName))
        .collect(Collectors.toList());
  }
}
