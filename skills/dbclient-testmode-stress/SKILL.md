---
name: dbclient-testmode-stress
description: >
  Generic logic for DBClient connectionstress test mode: DatabaseTester.connectionStress()
  implementation conventions, concurrent connection creation, and connection-leak troubleshooting.
  Use this skill when modifying the stress model or investigating connection-count issues.
---

# DBClient Test Mode: Connection Stress

## Call Chain

```
TestExecutor.executeTest()
  tester.connectionStress(connectionCount, duration)
```

Note: `connectionStress` does not go through `tester.connect()`; each Tester creates and closes many connections itself.

## Interface Signature

```java
String connectionStress(int connections, int duration);
```

## Default Parameters

- `connectionCount` defaults to `100`
- `duration` defaults to `60` seconds

## Generic Implementation Pattern

1. Use an `ExecutorService` to start `connections` tasks.
2. Each task loops: `connect()` -> optionally run a simple command -> `close()`.
3. Run for `duration` seconds, counting successful/failed connections and QPS.
4. After the test, call `releaseConnections()` to clean up.

## Core Files

| File | Responsibility |
|---|---|
| `src/main/java/com/apecloud/dbtester/commons/DatabaseTester.java` | `connectionStress()` interface |
| `src/main/java/com/apecloud/dbtester/commons/TestExecutor.java` | Parameter dispatch |
| `src/main/java/com/apecloud/dbtester/commons/DatabaseConnection.java` | Connection abstraction |
| `src/main/java/com/apecloud/dbtester/tester/RedisTester.java` | Pooled-connection stress reference |

## Minimal Example

```bash
java -jar build/libs/oneclient-1.0-all.jar \
  -h 127.0.0.1 -P 3306 -u root -p password \
  -e mysql -t connectionstress -c 200 -s 30
```

## Common Issues

| Symptom | Investigation Point |
|---|---|
| Connection count cannot reach target | Server `max_connections`, client file descriptors, JDBC pool config |
| Mass timeouts | Whether `DBConfig.duration` is long enough; whether connections are closed and exhausted |
| Redis stress anomaly | Whether JedisPool `setMaxTotal()` is smaller than `-c` |
| High memory/CPU | Whether the stress loop caches connections/results without releasing them |

## Maintenance Notes
- Changing the default `connectionCount` changes the stress baseline for all engines.
- `connectionStress` is usually one of the first capabilities to verify when adding a new engine.
