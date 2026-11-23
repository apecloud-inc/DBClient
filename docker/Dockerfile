FROM openjdk:11.0.16-jdk-slim AS builder

WORKDIR /app

COPY ./gradlew .
COPY ./gradlew.bat .
COPY ./build.gradle .
COPY ./settings.gradle .
COPY ./libs ./libs
COPY ./gradle ./gradle
COPY ./src ./src

# Limit Gradle memory and disable daemon to prevent OOM on arm64 emulation in CI runners
ENV GRADLE_OPTS="-Xmx1024m -Xms512m -Dorg.gradle.daemon=false"
ENV GRADLE_USER_HOME=/tmp/.gradle

RUN chmod +x gradlew && \
    ./gradlew --version --no-daemon && \
    ./gradlew build --no-daemon --max-workers=2 && \
    rm -rf ./gradle /tmp/.gradle ~/.gradle && \
    rm -rf build/distributions build/classes build/tmp

FROM eclipse-temurin:11-jre

WORKDIR /app

COPY --from=builder /app/build/libs/dbclient-1.0-all.jar .

ADD ./docker/run.sh /run.sh

RUN chmod +x /run.sh

ENTRYPOINT ["/run.sh"]

CMD []
