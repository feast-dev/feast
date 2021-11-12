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
package feast.common.logging.entry;

import com.google.auto.value.AutoValue;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.protobuf.Empty;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status.Code;
import java.lang.reflect.Type;
import java.util.UUID;

/** MessageAuditLogEntry records the handling of a Protobuf message by a service call. */
@AutoValue
public abstract class MessageAuditLogEntry extends AuditLogEntry {
  /** @return Id used to identify the service call that the log entry is recording */
  public abstract UUID getId();

  /** @return The name of the service that was used to handle the service call. */
  public abstract String getService();

  /** @return The name of the method that was used to handle the service call. */
  public abstract String getMethod();

  /**
   * @return The request Protobuf {@link Message} that was passed to the Service in the service
   *     call.
   */
  public abstract Message getRequest();

  /**
   * @return The response Protobuf {@link Message} that was passed to the Service in the service
   *     call. May be an {@link Empty} protobuf no request could be collected due to an error.
   */
  public abstract Message getResponse();

  /**
   * @return The authenticated identity that was assumed during the handling of the service call.
   *     For example, the user id or email that identifies the user making the call. Empty if the
   *     service call is not authenticated.
   */
  public abstract String getIdentity();

  /** @return The result status code of the service call. */
  public abstract Code getStatusCode();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setId(UUID id);

    public abstract Builder setComponent(String component);

    public abstract Builder setVersion(String component);

    public abstract Builder setKind(AuditLogEntryKind kind);

    public abstract Builder setService(String name);

    public abstract Builder setMethod(String name);

    public abstract Builder setRequest(Message request);

    public abstract Builder setResponse(Message response);

    public abstract Builder setIdentity(String identity);

    public abstract Builder setStatusCode(Code statusCode);

    public abstract MessageAuditLogEntry build();
  }

  public static MessageAuditLogEntry.Builder newBuilder() {
    return new AutoValue_MessageAuditLogEntry.Builder()
        .setKind(AuditLogEntryKind.MESSAGE)
        .setId(UUID.randomUUID());
  }

  @Override
  public String toJSON() {
    // GSON requires custom typeadapter (serializer) to convert Protobuf messages to JSON properly
    Gson gson =
        new GsonBuilder()
            .registerTypeAdapter(
                Message.class,
                new JsonSerializer<Message>() {
                  @Override
                  public JsonElement serialize(
                      Message message, Type type, JsonSerializationContext context) {
                    try {
                      String messageJSON = JsonFormat.printer().print(message);
                      return new JsonParser().parse(messageJSON);
                    } catch (InvalidProtocolBufferException e) {

                      throw new RuntimeException(
                          "Unexpected exception converting Protobuf to JSON", e);
                    }
                  }
                })
            .create();
    return gson.toJson(this);
  }
}
