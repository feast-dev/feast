FROM openjdk:8-slim

RUN apt-get update && apt-get install -y git maven

RUN git clone https://github.com/gojek/feast \
    && cd feast && mvn test || true \
    && cd .. && rm -rf feast

RUN apt-get install -y make curl

RUN curl -sSL -o /usr/local/bin/argo https://github.com/argoproj/argo/releases/download/v2.2.1/argo-linux-amd64 \
    && chmod +x /usr/local/bin/argo

COPY run.sh /usr/local/bin/

ENTRYPOINT ["run.sh"]