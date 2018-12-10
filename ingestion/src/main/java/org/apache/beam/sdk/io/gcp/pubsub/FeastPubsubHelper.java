/*
 * Copyright 2018 The Feast Authors
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
 *
 */

package org.apache.beam.sdk.io.gcp.pubsub;

import com.google.common.collect.Lists;
import com.google.protobuf.Message;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.OutgoingMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This PubSub helper exists because including the canonical client libraries can result in
 * dependency problems. We place it in the beam package because we need protected methods.
 */
public class FeastPubsubHelper {

  private final PipelineOptions options;
  private transient PubsubClient client;

  public FeastPubsubHelper(PipelineOptions options) {
    this.options = options;
  }

  private PubsubClient getClient() throws IOException {
    if (client == null) {
      client = PubsubJsonClient.FACTORY.newClient(null, null, options.as(PubsubOptions.class));
    }
    return client;
  }

  public int publish(TopicPath topicPath, Message message) throws IOException {
    List<OutgoingMessage> messages =
        Lists.newArrayList(
            new OutgoingMessage(
                message.toByteArray(), new HashMap<>(), System.currentTimeMillis(), null));
    return getClient().publish(topicPath, messages);
  }
}
