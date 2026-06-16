---
name: dbclient-engine-vector
description: >
  Vector databases in DBClient: Milvus and Qdrant.
  Use this skill when adding, debugging, or modifying such a Tester.
---

# DBClient Engine: Vector

## Covered Testers and Aliases

| Engine | Tester | Aliases |
|---|---|---|
| Milvus | `MilvusTester.java` | `milvus` |
| Qdrant | `QdrantTester.java` | `qdrant` |

## Key Dependencies (`build.gradle`)

```groovy
// Milvus
implementation 'io.milvus:milvus-sdk-java:2.5.9'
implementation 'io.perfmark:perfmark-api:0.26.0'

// Qdrant
implementation 'io.qdrant:client:1.12.0'
implementation 'org.apache.httpcomponents:httpclient:4.5.13'
implementation 'com.fasterxml.jackson.core:jackson-databind:2.13.0'
```

## Key Concerns

- Vector databases use concepts such as dimension, collection, and distance metric; the `query` parameter format is not SQL.
- `VectorGenerator` (`src/main/java/com/apecloud/dbtester/commons/VectorGenerator.java`) is used to generate test vectors.
- Qdrant result formatting has a special branch in `TestExecutor.formatQueryResult()`.

## Maintenance Notes
- Vector SDKs evolve quickly; ensure server and client version compatibility.
- When adding a new vector engine, check whether `VectorGenerator` needs to be extended.
