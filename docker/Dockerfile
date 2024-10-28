FROM openjdk:8-jdk-slim

WORKDIR /app

COPY ./gradlew ./gradlew.bat ./build.gradle ./settings.gradle .

COPY ./gradle ./gradle

COPY ./src ./src

RUN chmod +x gradlew && \
    ./gradlew --version && \
    ./gradlew build

RUN rm -rf .gradle && \
    rm -rf build/intermediates && \
    rm -rf build/classes && \
    rm -rf build/resources

ADD ./docker/run.sh /run.sh

ENTRYPOINT ["/run.sh"]

CMD []
