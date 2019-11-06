FROM openjdk:11-jdk

ARG REVISION=dev

COPY serving/target/feast-serving-$REVISION.jar /usr/share/feast/feast-serving.jar

ENTRYPOINT ["java", "-XX:+UnlockExperimentalVMOptions", "-jar", "/usr/share/feast/feast-serving.jar"]
