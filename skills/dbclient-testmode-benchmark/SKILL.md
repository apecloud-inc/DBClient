---
name: dbclient-testmode-benchmark
description: >
  Generic logic for DBClient benchmark test mode: DatabaseTester.bench() implementation conventions,
  concurrency model, and result calculation. Use this skill when modifying the performance-test
  framework or troubleshooting throughput/concurrency issues.
---

# DBClient Test Mode: Benchmark

## Call Chain

```
TestExecutor.executeTest()
  tester.connect()
  tester.bench(connection, query, iterations, concurrency)
```

## Interface Signature

```java
String bench(DatabaseConnection connection, String query, int iterations, int concurrency);
```

## Generic Implementation Pattern

Most `bench()` methods use:

1. An `ExecutorService` managing `concurrency` threads.
2. A `CountDownLatch` to wait for all threads ready/done.
3. Each thread executes `iterations / concurrency` times `execute(connection, query)`.
4. Aggregates elapsed time, OPS, average latency, and success/failure counts.

## Core Files

| File | Responsibility |
|---|---|
| `src/main/java/com/apecloud/dbtester/commons/DatabaseTester.java` | `bench()` interface |
| `src/main/java/com/apecloud/dbtester/commons/TestExecutor.java` | Dispatches to `bench()` |
| `src/main/java/com/apecloud/dbtester/tester/MySQLTester.java` | Reference JDBC benchmark implementation |
| `src/main/java/com/apecloud/dbtester/tester/RedisTester.java` | Reference non-JDBC benchmark implementation |

## Default Parameters

- `iterations` defaults to `1000`
- `concurrency` defaults to `10`

## Minimal Example

```bash
java -jar build/libs/dbclient-1.0-all.jar \
  -h 127.0.0.1 -P 3306 -u root -p password -d test \
  -e mysql -t benchmark -q "SELECT 1" -i 10000 -m 50
```

## Common Issues

| Symptom | Investigation Point |
|---|---|
| Throughput much lower than expected | Does each thread create a new connection? Is the connection reused? |
| High latency jitter | Missing warm-up, GC pauses, thread pool warm-up |
| Threads never finish | `ExecutorService.awaitTermination()` timeout settings |
| Unstable results | Whether `iterations` is divisible by `concurrency`; counting logic correctness |

## Maintenance Notes
- Changing the `bench()` return-value format affects benchmark output of all engines; update their skill examples.
