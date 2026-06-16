---
name: dbclient-troubleshooting
description: >
  Decision tree and common failure modes for DBClient. Use this skill first when a test run fails
  and you need to narrow down whether the issue is in CLI args, factory mapping, the engine Tester,
  result formatting, or dependencies.
---

# DBClient Troubleshooting

## Symptom: `Unsupported database type: <dbType>`

1. Check that `dbType` is registered in `TesterFactory.createTester()` (`dbclient-core`).
2. Check that the same alias is in `DBConfig.Builder.validate()` whitelist (`dbclient-core`).
3. Check spelling and case; aliases are lower-cased by the factory but the original value appears in error messages.

## Symptom: `Unsupported test type: <testType>` / `Test type not specified`

1. Verify `-t` is one of `query`, `benchmark`, `connectionstress`, `executionloop`.
2. If using `query` or `benchmark`, verify `-q` is provided and not empty.
3. For `connectionstress`, verify `-c` and `-s` are provided.
4. For `executionloop`, verify `-s` is provided.

## Symptom: `Invalid numeric parameter`

- Usually a missing or non-numeric `-P` port. `OneClient` does not default the port; it must be supplied on the CLI.

## Symptom: Connection refused / timeout

1. Verify `-h` and `-P` match the target service.
2. Verify network reachability (`telnet` / `nc`).
3. Check that the engine-specific driver/dependency is declared in `build.gradle` and packaged into the shadowJar.
4. For engines using local jars in `libs/`, confirm the jar exists.

## Symptom: `ClassNotFoundException` / `NoClassDefFoundError`

- The driver dependency is missing or not packaged. Run `gradle shadowJar` and inspect the jar contents:
  ```bash
  jar tf build/libs/oneclient-1.0-all.jar | grep -i mysql
  ```

## Symptom: Results look wrong (empty / garbled / wrong format)

1. Check `TestExecutor.formatQueryResult()` branches (`dbclient-testmode-query`).
2. For MongoDB, ensure the Tester returns `MongoDBResult`.
3. For Redis, ensure the command is one of the supported commands (`GET`, `SET`, `DEL`, `KEYS`).
4. For Qdrant, ensure the `result` column is populated.

## Symptom: Benchmark throughput or connection-stress count is too low

1. Check whether the Tester reuses connections or creates a new one per iteration.
2. Check pool settings (e.g., `JedisPool.setMaxTotal()` for Redis).
3. Verify server-side limits (`max_connections`, file descriptors).

## How to trace which Tester is actually used

```bash
# Add a temporary log or breakpoint in TesterFactory.createTester()
# Or run with a minimal query and check the connection information printed by OneClient
java -jar build/libs/oneclient-1.0-all.jar \
  -h 127.0.0.1 -P <port> -u <user> -p <pass> \
  -e <dbtype> -t query -q "SELECT 1"
```

## Core Diagnostic Files

| File | What to inspect |
|---|---|
| `src/main/java/OneClient.java` | Argument parsing and validation exceptions |
| `src/main/java/com/apecloud/dbtester/commons/TesterFactory.java` | dbType -> Tester mapping |
| `src/main/java/com/apecloud/dbtester/commons/DBConfig.java` | Field defaults and validation rules |
| `src/main/java/com/apecloud/dbtester/commons/TestExecutor.java` | testType dispatch and result formatting |
| `build.gradle` | Dependencies and shadowJar packaging |

## Maintenance Notes
- When adding a new failure mode, add it here.
- When changing error messages in `OneClient.java`, `TesterFactory.java`, or `TestExecutor.java`, keep the symptom strings in this skill in sync.
