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
package feast.core.grpc.interceptors;

import feast.core.metrics.GrpcMetrics;
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

/**
 * MonitoringInterceptor intercepts a GRPC call to provide a request latency historgram metrics in
 * the Prometheus client.
 */
public class MonitoringInterceptor implements ServerInterceptor {

  @Override
  public <ReqT, RespT> Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

    long startCallMillis = System.currentTimeMillis();
    String fullMethodName = call.getMethodDescriptor().getFullMethodName();
    String serviceName = MethodDescriptor.extractFullServiceName(fullMethodName);
    String methodName = fullMethodName.substring(fullMethodName.indexOf("/") + 1);

    return next.startCall(
        new SimpleForwardingServerCall<ReqT, RespT>(call) {
          @Override
          public void close(Status status, Metadata trailers) {
            GrpcMetrics.requestLatency
                .labels(serviceName, methodName, status.getCode().name())
                .observe((System.currentTimeMillis() - startCallMillis) / 1000f);
            super.close(status, trailers);
          }
        },
        headers);
  }
}
