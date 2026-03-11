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

FROM openjdk:11.0.16-jre-slim

RUN apt-get update && apt-get install -y tzdata

WORKDIR /app

# install FoundationDB client
RUN apt-get update && \
    apt-get install -y wget gnupg2 && \
    wget https://github.com/apple/foundationdb/releases/download/7.1.57/foundationdb-clients_7.1.57-1_amd64.deb && \
    dpkg -i foundationdb-clients_7.1.57-1_amd64.deb && \
    rm foundationdb-clients_7.1.57-1_amd64.deb && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# set FoundationDB cluster file
ENV FDB_CLUSTER_FILE=/usr/local/etc/foundationdb/fdb.cluster

COPY --from=builder /app/build/libs/oneclient-1.0-all.jar .

ADD ./docker/run.sh /run.sh

RUN chmod +x /run.sh

ENTRYPOINT ["/run.sh"]

CMD []
