package feast.core.config;

import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import org.lognet.springboot.grpc.GRpcServerBuilderConfigurer;
import org.springframework.stereotype.Component;

@Component
public class CoreGRpcServerBuilderConfig extends GRpcServerBuilderConfigurer {
  @Override
  public void configure(ServerBuilder<?> serverBuilder) {
    serverBuilder.addService(ProtoReflectionService.newInstance());
  }
}
