---
name: dbclient-engine-storage
description: >
  Document / object / graph / big-data storage engines in DBClient: MongoDB, MinIO,
  Hadoop, Hive, Vault, and Nebula. Use this skill when adding, debugging, or
  modifying such a Tester.
---

# DBClient Engine: Storage / NoSQL / Big Data

## Covered Testers and Aliases

| Engine | Tester | Aliases |
|---|---|---|
| MongoDB | `MongoDBTester.java` | `mongo`, `mongodb` |
| MinIO | `MinioTester.java` | `minio` |
| Hadoop | `HadoopTester.java` | `hadoop` |
| Hive | `HiveTester.java` | `hive` |
| Vault | `VaultTester.java` | `vault` |
| Nebula | `NebulaTester.java` | `nebula` |

## Key Dependencies (`build.gradle`)

```groovy
// MongoDB
implementation 'org.mongodb:mongodb-driver-sync:4.6.1'
implementation 'org.mongodb:mongodb-driver-reactivestreams:4.6.1'

// MinIO
implementation 'io.minio:minio:8.5.11'

// Hadoop
implementation 'org.apache.hadoop:hadoop-client:3.3.6'

// Hive
implementation('org.apache.hive:hive-jdbc:3.1.3') {
    exclude group: 'org.apache.hadoop'
    exclude group: 'org.apache.hbase'
}

// Nebula
implementation 'com.vesoft:client:3.8.4'
implementation 'com.alibaba:fastjson:2.0.57'
```

## Key Concerns

- MongoDB uses `MongoDBResult` to wrap `List<Document>`; `TestExecutor.formatQueryResult()` has a special JSON branch for `mongodb`.
- MinIO is object storage; the query parameter usually follows bucket/key semantics; `-B` / `--bucket` and `-k` / `--key` are the dedicated parameters.
- Hive excludes hadoop/hbase to avoid conflicts with `hadoop-client`.
- Nebula is a graph database; its connection and query semantics differ from JDBC.

## Minimal Verification Command (MongoDB Example)

```bash
java -jar build/libs/oneclient-1.0-all.jar \
  -h 127.0.0.1 -P 27017 -u admin -p password -d test \
  -e mongodb -t query -q "db.collection.find().limit(1)"
```

## Maintenance Notes
- When adding a new NoSQL / object storage engine, add a result-formatting branch in `TestExecutor` if needed.
- When adding Hadoop/Hive dependencies, check whether a local `libs/` jar is also required.
