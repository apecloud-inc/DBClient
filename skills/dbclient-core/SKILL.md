---
name: dbclient-core
description: >
  Cross-cutting core mechanisms of DBClient: CLI argument parsing, DBConfig, TesterFactory,
  TestExecutor, and the DatabaseTester interface. Use this skill when modifying generic flows
  or adding engine registration, validation, or dependencies.
---

# DBClient Core

## Core Files

| File | Responsibility |
|---|---|
| `src/main/java/OneClient.java` | CLI entry: parses arguments, builds DBConfig, invokes TestExecutor |
| `src/main/java/com/apecloud/dbtester/commons/DBConfig.java` | Configuration object (Builder pattern) and whitelist validation |
| `src/main/java/com/apecloud/dbtester/commons/TesterFactory.java` | Creates the matching Tester from dbType |
| `src/main/java/com/apecloud/dbtester/commons/TestExecutor.java` | Dispatches execution by testType |
| `src/main/java/com/apecloud/dbtester/commons/DatabaseTester.java` | Interface for all Testers |
| `src/main/java/com/apecloud/dbtester/commons/DatabaseConnection.java` | Connection abstraction |
| `src/main/java/com/apecloud/dbtester/commons/DatabaseConnectionFactory.java` | Connection factory |
| `src/main/java/com/apecloud/dbtester/commons/ResultSetPrinter.java` | Result formatting and printing helper |
| `src/main/java/com/apecloud/dbtester/commons/QueryResult.java` | Query result wrapper |
| `build.gradle` | Dependency declarations and shadowJar packaging |

## CLI Parameters Quick Reference

| Short | Long | Default | Description |
|---|---|---|---|
| `-h` | `--host` | `127.0.0.1` | Database host |
| `-P` | `--port` | required (no CLI default) | Port |
| `-u` | `--user` | `""` | User name |
| `-p` | `--password` | `""` | Password |
| `-d` | `--database` | `""` | Database name |
| `-o` | `--org` | `""` | Organization name required by some databases |
| `-e` | `--dbtype` | `""` (required) | Database type, see TesterFactory |
| `-t` | `--test` | `""` (required) | `query` / `connectionstress` / `benchmark` / `executionloop` |
| `-q` | `--query` | `""` | SQL / query statement |
| `-c` | `--connections` | `100` | Number of connections for stress test |
| `-s` | `--duration` | `60` | Duration in seconds |
| `-i` | `--iterations` | `1000` | Benchmark iteration count |
| `-m` | `--concurrency` | `10` | Benchmark concurrency |
| `-I` | `--interval` | `1` | Loop test report interval in seconds; `0` disables intermediate reports |
| `-a` | `--accessmode` | `mysql` | Access syntax mode: `mysql` / `postgresql` / `oracle` / `redis` / `mongodb` / `influxdb` / `prometheus` |

Other specialized parameters:
- `-M` / `--master`: Redis sentinel master
- `-S` / `--sentinelPassword`: Redis sentinel password
- `-k` / `--key`: Database key
- `-T` / `--topic`: Topic
- `-B` / `--bucket`: Bucket
- `-C` / `--cluster`: Cluster
- `-tb` / `--table`: Table name

## DBConfig Validation Rules

Key validations in `DBConfig.Builder.validate()`:

- `dbType` and `testType` are required.
- `testType` must be one of `query` / `benchmark` / `connectionstress` / `executionloop`.
- `query` is required when `testType` is `query` or `benchmark`.
- `connectionCount` / `duration` are required when `testType` is `connectionstress`.
- `duration` is required when `testType` is `executionloop`.
- `dbType` must be in the DBConfig whitelist, which should match the case list in `TesterFactory`.

## TesterFactory Mapping Convention

When adding or modifying dbType aliases, append them to the switch in `TesterFactory.createTester()`. For example, MySQL-protocol-compatible engines all map to `MySQLTester`:

```java
case "foxlake":
case "greatdb":
case "greatsql":
case "greptime":
case "greptimedb":
case "mariadb":
case "mysql":
case "polardbx":
case "tidb":
    return new MySQLTester(config);
```

## TestExecutor Execution Flow

1. Validate `DBConfig`.
2. `tester.connect()`.
3. Dispatch by `testType`:
   - `query`: call `tester.execute()`, then `formatQueryResult()`
   - `benchmark`: call `tester.bench()`
   - `connectionstress`: call `tester.connectionStress()`
   - `executionloop`: call `tester.executionLoop()`
4. Close `DatabaseConnection` in `finally`.

Result formatting has special branches for MongoDB and Redis; all others are printed as a tab-separated `ResultSet` table.

## Generic Coding Conventions

- Tester class names use the `XxxTester` suffix and implement `DatabaseTester`.
- Provide both a no-arg constructor and a `(DBConfig dbConfig)` constructor.
- Load JDBC drivers explicitly with `Class.forName()`.
- Convert connection/execution exceptions to `IOException`.
- Use `ExecutorService` + `CountDownLatch` for concurrent tests.
- Use `SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")` for timestamps.
- New dependencies must be declared in `build.gradle` and packaged by `shadowJar`.

## Maintenance Notes

Sync this skill when modifying any of the following:
- `OneClient.java`: argument parsing and defaults
- `DBConfig.java`: fields, Builder, validation whitelist
- `TesterFactory.java`: dbType mapping
- `TestExecutor.java`: test-type dispatching and result formatting
- `DatabaseTester.java`: interface changes
- `build.gradle`: dependency coordinates
