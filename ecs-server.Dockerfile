# Build maven project
FROM maven:3.8.3-openjdk-17 AS builder
ADD pom.xml pom.xml

ADD ecs-server/src ecs-server/src
ADD ecs-server/pom.xml ecs-server/pom.xml

ADD kv-client/src kv-client/src
ADD kv-client/pom.xml kv-client/pom.xml

ADD kv-server/src kv-server/src
ADD kv-server/pom.xml kv-server/pom.xml

RUN mvn clean package -pl ecs-server -am

FROM openjdk:21-slim
WORKDIR ecs-server
COPY ecs-server/logs/* logs/*
#COPY ecs-server/data data
COPY --from=builder ecs-server/target/ecs-server.jar ecs-server.jar
ENTRYPOINT ["java", "-jar", "ecs-server.jar"]
