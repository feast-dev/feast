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
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import feast.serving.ServingAPIProto.QueryFeatures.Request;
import feast.serving.ServingAPIProto.RequestDetail;
import feast.serving.ServingAPIProto.TimestampRange;

public class RequestHelper {
    private RequestHelper() {}

    public static void validateRequest(Request request) {
        // entity name shall present
        if (Strings.isNullOrEmpty(request.getEntityName())) {
            throw new IllegalArgumentException("entity name must be set");
        }

        // entity id list shall not be empty
        if (request.getEntityIdList().size() <= 0) {
            throw new IllegalArgumentException("entity ID must be provided");
        }

        // request detail shall not be empty
        if (request.getRequestDetailsList().size() <= 0) {
            throw new IllegalArgumentException("request details must be provided");
        }

        // feature id in each request detail shall have same entity name
        String entityName = request.getEntityName();
        for (RequestDetail requestDetail : request.getRequestDetailsList()) {
            String featureId = requestDetail.getFeatureId();
            if (!featureId.substring(0, featureId.indexOf(".")).equals(entityName)) {
                throw new IllegalArgumentException(
                        "entity name of all feature ID in request details must be: " + entityName);
            }
        }
    }

    public static Request checkTimestampRange(Request request) {
        Request.Builder requestBuilder = Request.newBuilder(request);

        Timestamp defaultTs =
                Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000).build();

        // default timestamp range
        if (!request.hasTimestampRange()) {
            requestBuilder.setTimestampRange(
                    TimestampRange.newBuilder().setStart(defaultTs).setEnd(defaultTs).build());
        } else if (request.getTimestampRange().getStart().getSeconds() == 0
                && request.getTimestampRange().getStart().getNanos() == 0) {
            Timestamp end = request.getTimestampRange().getEnd();
            requestBuilder.setTimestampRange(
                    TimestampRange.newBuilder(request.getTimestampRange()).setStart(end));
        } else if (request.getTimestampRange().getEnd().getSeconds() == 0
                && request.getTimestampRange().getEnd().getNanos() == 0) {
            requestBuilder.setTimestampRange(
                    TimestampRange.newBuilder(request.getTimestampRange()).setEnd(defaultTs));
        }

        Request newRequest = requestBuilder.build();
        Timestamp start = newRequest.getTimestampRange().getStart();
        Timestamp end = newRequest.getTimestampRange().getEnd();

        if (Timestamps.compare(start, end) > 0) {
            throw new IllegalArgumentException(
                    "'end' of timestampRange must be before or same time as 'start'");
        }

        return newRequest;
    }
}
