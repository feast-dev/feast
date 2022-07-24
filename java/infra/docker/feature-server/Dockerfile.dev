FROM openjdk:11-jre as production
ARG REVISION=dev
#
# Download grpc_health_probe to run health check for Feast Serving
# https://kubernetes.io/blog/2018/10/01/health-checking-grpc-servers-on-kubernetes/
#
RUN wget -q https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.3.1/grpc_health_probe-linux-amd64 \
         -O /usr/bin/grpc-health-probe && \
    chmod +x /usr/bin/grpc-health-probe
ADD $PWD/serving/target/feast-serving-$REVISION-exec.jar /opt/feast/feast-serving.jar
CMD ["java",\
     "-Xms1024m",\
     "-Xmx1024m",\
     "-jar",\
     "/opt/feast/feast-serving.jar"]
