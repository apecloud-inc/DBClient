FROM openjdk:11.0.16-jdk-slim AS builder

WORKDIR /app

COPY ./gradlew .
COPY ./gradlew.bat .
COPY ./build.gradle .
COPY ./settings.gradle .
COPY ./libs ./libs
COPY ./gradle ./gradle
COPY ./src ./src

RUN chmod +x gradlew && \
    ./gradlew --version && \
    ./gradlew build && \
    rm -rf ./gradle && \
    rm -rf build/distributions && \
    rm -rf build/classes && \
    rm -rf build/tmp

FROM eclipse-temurin:11-jre

WORKDIR /app

COPY --from=builder /app/build/libs/dbclient-1.0-all.jar .

ADD ./docker/run.sh /run.sh

RUN chmod +x /run.sh

ENTRYPOINT ["/run.sh"]

CMD []
