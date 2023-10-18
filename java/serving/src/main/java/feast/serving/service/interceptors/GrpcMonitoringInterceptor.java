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
package feast.serving.service.interceptors;

import feast.serving.util.Metrics;
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.util.Optional;

/**
 * GrpcMonitoringInterceptor intercepts GRPC calls to provide request latency histogram metrics in
 * the Prometheus client.
 */
public class GrpcMonitoringInterceptor implements ServerInterceptor {

  @Override
  public <ReqT, RespT> Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

    long startCallMillis = System.currentTimeMillis();
    String fullMethodName = call.getMethodDescriptor().getFullMethodName();
    String methodName = fullMethodName.substring(fullMethodName.indexOf("/") + 1);

    GrpcMonitoringContext.getInstance().clearProject();

    return next.startCall(
        new SimpleForwardingServerCall<ReqT, RespT>(call) {
          @Override
          public void close(Status status, Metadata trailers) {
            Optional<String> projectName = GrpcMonitoringContext.getInstance().getProject();

            Metrics.requestLatency
                .labels(methodName, projectName.orElse(""))
                .observe((System.currentTimeMillis() - startCallMillis) / 1000f);
            Metrics.grpcRequestCount.labels(methodName, status.getCode().name()).inc();
            super.close(status, trailers);
          }
        },
        headers);
  }
}
