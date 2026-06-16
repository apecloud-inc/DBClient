---
name: dbclient-testmode-loop
description: >
  Generic logic for DBClient executionloop test mode: DatabaseTester.executionLoop()
  periodic execution and intermediate reporting. Use this skill when modifying long-running
  tests or scheduled report logic.
---

# DBClient Test Mode: Execution Loop

## Call Chain

```
TestExecutor.executeTest()
  tester.connect()
  tester.executionLoop(connection, query, duration, interval, database, table)
```

## Interface Signature

```java
String executionLoop(
    DatabaseConnection connection,
    String query,
    int duration,
    int interval,
    String database,
    String table
);
```

## Parameter Meanings

| Parameter | Default | Description |
|---|---|---|
| `duration` | `60` seconds | Total loop duration |
| `interval` | `1` second | Intermediate report interval; `0` disables intermediate reports |
| `database` | `""` | Database name needed by some engines |
| `table` | `""` | Table name needed by some engines |
| `query` | `""` | Query to execute in loop; some engines may ignore it |

## Generic Implementation Pattern

1. Record start time.
2. Periodically execute the query/operation within `duration` seconds.
3. Print intermediate statistics (success/failure/latency) every `interval` seconds.
4. Return final aggregated total count, average latency, and failure rate.

## Core Files

| File | Responsibility |
|---|---|
| `src/main/java/com/apecloud/dbtester/commons/DatabaseTester.java` | `executionLoop()` interface |
| `src/main/java/com/apecloud/dbtester/commons/TestExecutor.java` | Parameter validation and dispatch |

## Minimal Example

```bash
java -jar build/libs/oneclient-1.0-all.jar \
  -h 127.0.0.1 -P 3306 -u root -p password -d test \
  -e mysql -t executionloop -q "SELECT 1" -s 60 -I 10
```

## Common Issues

| Symptom | Investigation Point |
|---|---|
| No intermediate reports | Whether `interval` is set to 0 |
| Ends early | Whether `duration` is parsed correctly (note that `-s` is shared with stress) |
| Inaccurate statistics | Whether counters are thread-safe; whether interval boundaries are double-counted |
| Some engines unsupported | Whether that Tester's `executionLoop()` is fully implemented |

## Maintenance Notes
- `DBConfig.validate()` requires `duration`; sync `dbclient-core` when changing it.
