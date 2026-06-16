---
name: dbclient-engine-ts
description: >
  Time-series engines in DBClient: InfluxDB, TDengine, VictoriaMetrics, VictoriaLogs, and Loki.
  Use this skill when adding, debugging, or modifying such a Tester.
---

# DBClient Engine: Time-Series

## Covered Testers and Aliases

| Engine | Tester | Aliases |
|---|---|---|
| InfluxDB | `InfluxDBTester.java` | `influx`, `influxdb` |
| TDengine | `TDEngineTester.java` | `tdengine`, `td`, `taos` |
| VictoriaMetrics | `VictoriaMetricsTester.java` | `victoria-metrics`, `victoriametrics`, `vm` |
| VictoriaLogs | `VictoriaLogsTester.java` | `victoria-logs`, `victorialogs` |
| Loki | `LokiTester.java` | `loki` |

## Key Dependencies (`build.gradle`)

```groovy
// InfluxDB
implementation 'com.influxdb:influxdb-client-java:3.2.0'

// TDengine
implementation 'com.taosdata.jdbc:taos-jdbcdriver:3.3.0'
```

## Key Concerns

- InfluxDB uses the InfluxDB Java Client; whether it supports Flux, InfluxQL, or line protocol depends on the Tester implementation.
- TDengine uses a JDBC driver; the URL format is usually `jdbc:TAOS://host:port/db`.
- VictoriaMetrics and Loki usually use HTTP/REST with PromQL/LogQL query syntax.

## Minimal Verification Command (InfluxDB Example)

```bash
java -jar build/libs/oneclient-1.0-all.jar \
  -h 127.0.0.1 -P 8086 -u admin -p password -d test \
  -e influxdb -t query -q "SHOW DATABASES"
```

## Maintenance Notes
- When adding a new time-series engine, consider reusing an HTTP REST template first.
- The TDengine JDBC driver major version must match the server.
