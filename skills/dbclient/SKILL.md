---
name: dbclient
description: >
  Entry index for DBClient (OneClient), a unified database testing CLI client.
  Start here when you are unsure which sub-skill to use.
---

# DBClient / OneClient

## Project Overview
A unified Java command-line database testing client supporting connection test, query execution, connection stress test, benchmark, and execution-loop test.

Entry point: `src/main/java/OneClient.java`

## Choose a Skill by Task

| What you want to do | Skill to use |
|---|---|
| Modify CLI parameters, DBConfig, TesterFactory mapping, or TestExecutor generic logic | `dbclient-core` |
| Debug or extend a specific database engine | `dbclient-engine-*` |
| Modify the generic behavior of a test mode (query / benchmark / connectionstress / executionloop) | `dbclient-testmode-*` |
| Integrate a new database from scratch | `dbclient-add-engine` |
| Troubleshoot a failed test run | `dbclient-troubleshooting` |

## Engine Family Skills Quick Reference

| Skill | Covered engines |
|---|---|
| `dbclient-engine-relational` | MySQL / MariaDB / TiDB / OceanBase / PolarDB-X / GreatSQL / GreatDB / GreptimeDB / FoxLake / PostgreSQL / OpenTenBase / Oracle / SQLServer / Dameng / GaussDB / OpenGauss / Kingbase / Vastbase / MogDB / Gbase8c / ClickHouse / StarRocks / Doris / SelectDB |
| `dbclient-engine-kv` | Redis / RedisCluster / Redis Sentinel / Camellia-Redis-Proxy / Etcd / Zookeeper |
| `dbclient-engine-mq` | Kafka / Pulsar / RabbitMQ / RocketMQ |
| `dbclient-engine-search` | Elasticsearch 8.x / OpenSearch |
| `dbclient-engine-vector` | Milvus / Qdrant |
| `dbclient-engine-ts` | InfluxDB / TDengine / VictoriaMetrics / VictoriaLogs / Loki |
| `dbclient-engine-storage` | MongoDB / MinIO / Hadoop / Hive / Vault / Nebula |

## Quick Build and Run

```bash
gradle build
gradle shadowJar
java -jar build/libs/oneclient-1.0-all.jar \
  -h 127.0.0.1 -P 3306 -u root -p password -d test \
  -e mysql -t query -q "SELECT 1"
```

## Maintenance Notes
- After modifying `OneClient.java`, `DBConfig.java`, `TesterFactory.java`, or `build.gradle`, sync `dbclient-core`.
- After modifying an engine alias, dependency, or special parameter, sync the corresponding `dbclient-engine-*` skill.
- When adding a new engine, sync both `dbclient-add-engine` and the relevant engine-family skill.
