FROM openjdk:8-slim

RUN apt-get update && apt-get install -y git maven

RUN git clone https://github.com/gojek/feast \
    && cd feast && mvn test || true \
    && cd .. && rm -rf feast

RUN apt-get install -y make

COPY run.sh /usr/local/bin/

ENTRYPOINT ["run.sh"]