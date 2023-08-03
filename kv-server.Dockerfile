# Build maven project
FROM maven:3.8.3-openjdk-17 AS builder
ADD pom.xml pom.xml

ADD kv-server/src kv-server/src
ADD kv-server/pom.xml kv-server/pom.xml

ADD kv-client/src kv-client/src
ADD kv-client/pom.xml kv-client/pom.xml

ADD ecs-server/src ecs-server/src
ADD ecs-server/pom.xml ecs-server/pom.xml

RUN mvn clean package -pl kv-server -am

FROM openjdk:21-slim
WORKDIR kv-server
COPY kv-server/logs/* logs/*
COPY kv-server/data data
COPY --from=builder kv-server/target/kv-server.jar kv-server.jar
ENTRYPOINT ["java", "-jar", "kv-server.jar"]
