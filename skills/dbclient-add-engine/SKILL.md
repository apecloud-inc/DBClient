---
name: dbclient-add-engine
description: >
  Complete checklist and template for adding a new database or middleware engine to DBClient.
  Use this skill when you need to introduce a new dbType.
---

# DBClient: Add a New Engine

## Preliminary Check

1. **Is it protocol-compatible?**
   - MySQL protocol -> try reusing `MySQLTester` first
   - PostgreSQL protocol -> try reusing `PostgreSQLTester` first
   - Neither -> add a dedicated `XxxTester`
2. **Does it have an official Java SDK or JDBC driver?**
3. **Which testTypes should be supported?** At least implement `query` + `connectionstress`.

## Change Checklist

| # | File / Directory | Change |
|---|---|---|
| 1 | `src/main/java/com/apecloud/dbtester/tester/XxxTester.java` | Create the Tester implementing `DatabaseTester` |
| 2 | `src/main/java/com/apecloud/dbtester/commons/TesterFactory.java` | Register dbType aliases in the switch |
| 3 | `src/main/java/com/apecloud/dbtester/commons/DBConfig.java` | Append dbType aliases to the `validate()` whitelist |
| 4 | `build.gradle` | Add the corresponding Maven dependency or local jar |
| 5 | Relevant `skills/dbclient-engine-*/SKILL.md` | Add engine description, aliases, and dependencies |
| 6 | `skills/dbclient-add-engine/SKILL.md` | Update the template or known pitfalls |

## Minimal Tester Skeleton

```java
package com.apecloud.dbtester.tester;

import com.apecloud.dbtester.commons.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class XxxTester implements DatabaseTester {
    private final DBConfig dbConfig;
    private final List<DatabaseConnection> connections = new ArrayList<>();

    public XxxTester() {
        this.dbConfig = null;
    }

    public XxxTester(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    @Override
    public DatabaseConnection connect() throws IOException {
        if (dbConfig == null) {
            throw new IllegalStateException("DBConfig not provided");
        }
        // TODO: create a connection and return a DatabaseConnection subclass
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public QueryResult execute(DatabaseConnection connection, String query) throws IOException {
        // TODO: execute the query and wrap it into a QueryResult
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public String bench(DatabaseConnection connection, String query, int iterations, int concurrency) {
        // TODO: run the query concurrently for the given iterations
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public String connectionStress(int connections, int duration) {
        // TODO: loop creating and closing the given number of connections for the duration
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public String executeTest() throws IOException {
        return TestExecutor.executeTest(this, dbConfig);
    }

    @Override
    public String executionLoop(DatabaseConnection connection, String query,
                                int duration, int interval, String database, String table) {
        // TODO: execute periodically and print intermediate reports
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void releaseConnections() {
        for (DatabaseConnection conn : connections) {
            try { conn.close(); } catch (Exception ignored) {}
        }
        connections.clear();
    }
}
```

## Registration Example

Append to `TesterFactory.createTester()`:

```java
case "xxx":
case "xxxdb":
    return new XxxTester(config);
```

Append the same aliases to the dbType whitelist in `DBConfig.Builder.validate()`.

## Dependency Example

If the SDK is on Maven Central:

```groovy
implementation 'com.example:xxx-client:1.0.0'
```

If using a local jar:

```groovy
implementation files('libs/xxx-client.jar')
```

## Smoke Test

```bash
gradle shadowJar
java -jar build/libs/oneclient-1.0-all.jar \
  -h 127.0.0.1 -P <port> -u <user> -p <pass> -d <db> \
  -e xxx -t query -q "<minimal query>"
```

## Recommended Next Steps

- Get `query` working first.
- Then implement `connectionStress` to verify connection leaks.
- Finally implement `benchmark` and `executionloop` as needed.

## Maintenance Notes
- Keep `TesterFactory` and the `DBConfig` whitelist consistent when adding new aliases.
- If the new engine needs special result formatting, update `TestExecutor.formatQueryResult()`.
