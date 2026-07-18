---
name: dbclient-testmode-query
description: >
  Generic logic for DBClient query test mode: DatabaseTester.execute(), QueryResult wrapping,
  and format branches in TestExecutor.formatQueryResult(). Use this skill when modifying
  query execution or result output for any engine.
---

# DBClient Test Mode: Query

## Call Chain

```
DBClient.createConfig()
  -> TestExecutor.executeTest(tester, config)
    -> tester.connect()
    -> tester.execute(connection, query)
    -> formatQueryResult(queryResult, dbType)
```

## Core Files

| File | Responsibility |
|---|---|
| `src/main/java/com/apecloud/dbtester/commons/DatabaseTester.java` | `execute()` interface |
| `src/main/java/com/apecloud/dbtester/commons/QueryResult.java` | Unified query result wrapper |
| `src/main/java/com/apecloud/dbtester/commons/MongoDBResult.java` | MongoDB-specific result wrapper |
| `src/main/java/com/apecloud/dbtester/commons/TestExecutor.java` | `formatQueryResult()` branches |
| `src/main/java/com/apecloud/dbtester/commons/ResultSetPrinter.java` | Table printing helper |

## formatQueryResult Branches

1. `mongodb`: Converts `MongoDBResult.getDocuments()` into a JSON array.
2. `redis`: Joins `getRawResults()` with newlines; empty results return a hint.
3. Qdrant: Reads the `result` column from the `ResultSet` and concatenates.
4. Generic JDBC `ResultSet`: Prints column names + rows (tab-separated).
5. No `ResultSet`: Returns `Update count: N`.

## Common Issues

| Symptom | Investigation Point |
|---|---|
| Empty result | Check whether the query matches; Redis explicitly hints key may not exist |
| Misaligned format | `ResultSetMetaData.getColumnName(i)` and `rs.getString(i)` use 1-based indexes |
| MongoDB error | Confirm the returned type is `MongoDBResult`, not a generic `QueryResult` |
| Qdrant empty result | See the `dbType.toLowerCase().equals("qdrant")` branch in `TestExecutor` |

## Minimal Example

```bash
java -jar build/libs/dbclient-1.0-all.jar \
  -h 127.0.0.1 -P 3306 -u root -p password -d test \
  -e mysql -t query -q "SELECT 1 AS one"
```

## Maintenance Notes
- When adding an engine that needs special formatting, update `TestExecutor.formatQueryResult()`.
- When modifying the `QueryResult` interface, update every Tester result wrapper accordingly.
