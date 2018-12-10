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
package feast.serving.grpc;

import static feast.serving.util.StatsUtil.REMOTE_ADDRESS;
import static io.grpc.Grpc.TRANSPORT_ATTR_REMOTE_ADDR;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import java.net.SocketAddress;
import lombok.extern.slf4j.Slf4j;
import org.lognet.springboot.grpc.GRpcGlobalInterceptor;

/**
 * Interceptor for retrieving {@link io.grpc.Grpc#TRANSPORT_ATTR_REMOTE_ADDR} and storing it in
 * current Context.
 */
@Slf4j
@GRpcGlobalInterceptor
public class RemoteAddressInterceptor implements ServerInterceptor {

  @Override
  public <ReqT, RespT> Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
      ServerCallHandler<ReqT, RespT> next) {
    Context context = Context.current();
    try {
      SocketAddress client = call.getAttributes().get(TRANSPORT_ATTR_REMOTE_ADDR);
      context = context.withValue(REMOTE_ADDRESS, client);
    } catch (Exception e) {
      log.error("Unable to get remote address", e);
    }
    return Contexts.interceptCall(context, call, headers, next);
  }
}
