---
name: dbclient-engine-relational
description: >
  Relational / JDBC-protocol-compatible engines in DBClient.
  Covers MySQL-compatible, PostgreSQL-compatible, Oracle, SQLServer, domestic / regulated databases,
  and analytical databases such as StarRocks, Doris, and ClickHouse.
  Use this skill when adding, debugging, or modifying such a Tester.
---

# DBClient Engine: Relational

## Covered Testers and Aliases

### MySQL-protocol compatible -> `MySQLTester.java`
Aliases: `mysql`, `mariadb`, `tidb`, `polardbx`, `greatsql`, `greatdb`, `greptime` (`greptimedb`), `foxlake`

### PostgreSQL-protocol compatible -> `PostgreSQLTester.java`
Aliases: `postgresql` (`postgres` / `pg`), `opentenbase`

### Dedicated Testers

| Engine | Tester | Aliases |
|---|---|---|
| OceanBase | `OceanbaseTester.java` | `oceanbase`, `ob` |
| Oracle | `OracleTester.java` | `oracle` |
| SQLServer | `SQLServerTester.java` | `sqlserver`, `mssql` |
| Dameng | `DamengTester.java` | `dameng`, `damengdb`, `dm`, `dmdb` |
| GaussDB | `GaussdbTester.java` | `gaussdb` |
| OpenGauss | `OpenGaussTester.java` | `opengauss` |
| Kingbase | `KingbaseTester.java` | `kingbase` |
| Vastbase | `VastbaseTester.java` | `vastbase` |
| MogDB | `MogDBTester.java` | `mogdb` |
| Gbase8c | `Gbase8cTester.java` | `gbase`, `gbase8c` |
| ClickHouse | `ClickHouseTester.java` | `clickhouse`, `ck` |
| StarRocks | `StarRocksTester.java` | `starrocks`, `sr` |
| Doris | `DorisTester.java` | `doris` |
| SelectDB | `SelectDBTester.java` | `selectdb` |

## Key Dependencies (`build.gradle`)

```groovy
// MySQL / MariaDB
implementation 'mysql:mysql-connector-java:8.0.28'
implementation 'org.mariadb.jdbc:mariadb-java-client:3.3.3'

// PostgreSQL
implementation 'org.postgresql:postgresql:42.0.0'

// Oracle
implementation 'com.oracle.database.jdbc:ojdbc8:19.3.0.0'

// SQLServer
implementation 'com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11'

// Dameng
implementation 'com.dameng:Dm7JdbcDriver18:7.6.0.165'

// GaussDB / OpenGauss
implementation files('libs/opengaussjdbc.jar')
implementation 'org.opengauss:opengauss-jdbc:6.0.0-RC1-og'

// Kingbase
implementation 'cn.com.kingbase:kingbase8:8.6.0'

// MogDB
implementation 'io.mogdb:mogdb-jdbc:5.0.0.8.mg'

// Gbase8c (local jar)
implementation files('libs/GBase8cV5_JDBC_3.0.0/gbase8c-jdbc-3.0.0B02.jar')

// OceanBase
implementation files('libs/oceanbase-client-2.4.8.jar')

// ClickHouse
implementation 'ru.yandex.clickhouse:clickhouse-jdbc:0.3.2'
```

## Key Concerns

- Most relational Testers reuse `DatabaseConnection` plus JDBC `ResultSet`.
- JDBC URL formats, authentication methods, and schema/account concepts vary widely across databases.
- The aliases `ob` / `oceanbase` appear in both `MySQLTester` and `OceanbaseTester`; confirm which Tester is actually hit when debugging.
- When adding a new domestic database, prefer reusing `MySQLTester` or `PostgreSQLTester`; add a dedicated Tester only if the protocol differs significantly.

## Minimal Verification Command (MySQL Example)

```bash
java -jar build/libs/dbclient-1.0-all.jar \
  -h 127.0.0.1 -P 3306 -u root -p password -d test \
  -e mysql -t query -q "SELECT 1"
```

## Maintenance Notes
- Sync this skill after modifying alias mappings in `TesterFactory.java`.
- Sync the "Key Dependencies" section after changing driver versions in `build.gradle`.
