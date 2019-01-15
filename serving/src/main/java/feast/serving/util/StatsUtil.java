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
package feast.serving.util;

import com.google.common.base.Strings;
import feast.serving.ServingAPIProto.QueryFeaturesRequest;
import io.grpc.Context;
import io.grpc.Context.Key;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for statistics.
 */
public class StatsUtil {

  public static final Key<SocketAddress> REMOTE_ADDRESS = Context.key("remote-address");

  private StatsUtil() {
  }

  /**
   * Create Statsd Tag for a request.
   * <p>The tags contain information about feature's ids of the request and the client requesting
   * it.
   */
  public static String[] makeStatsdTags(QueryFeaturesRequest request) {
    List<String> featureTags = makeFeatureTags(request);
    String remoteAddrTag = makeRemoteAddressTag();
    String[] tags = featureTags.toArray(new String[featureTags.size() + 1]);
    tags[featureTags.size()] = remoteAddrTag;
    return tags;
  }

  private static List<String> makeFeatureTags(QueryFeaturesRequest request) {
    List<String> tags = new ArrayList<>(request.getFeatureIdCount());
    for (String featureId : request.getFeatureIdList()) {
      if (Strings.isNullOrEmpty(featureId)) {
        continue;
      }
      String featureTag = makeFeatureTag(featureId);
      tags.add(featureTag);
    }
    return tags;
  }

  private static String makeFeatureTag(String featureId) {
    return "feature:" + featureId;
  }

  private static String makeRemoteAddressTag() {
    SocketAddress socketAddress = REMOTE_ADDRESS.get();
    if (socketAddress == null) {
      return "remote:unknown";
    }
    return "remote:" + socketAddress.toString();
  }
}
