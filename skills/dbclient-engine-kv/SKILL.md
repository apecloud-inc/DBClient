---
name: dbclient-engine-kv
description: >
  KV / coordination engines in DBClient: Redis, RedisCluster, Redis Sentinel,
  Camellia-Proxy, Etcd, and Zookeeper. Use this skill when adding, debugging,
  or modifying such a Tester.
---

# DBClient Engine: KV / Coordination

## Covered Testers and Aliases

| Engine | Tester | Aliases |
|---|---|---|
| Redis | `RedisTester.java` | `redis`, `sentinelredis`, `camellia-proxy`, `camellia-redis-proxy` |
| Redis Cluster | `RedisClusterTester.java` | `redis-cluster` |
| Etcd | `EtcdTester.java` | `etcd` |
| Zookeeper | `ZookeeperTester.java` | `zk`, `zookeeper` |

## Key Dependencies (`build.gradle`)

```groovy
implementation 'redis.clients:jedis:3.7.0'  # also used by RedisClusterTester
implementation 'org.apache.zookeeper:zookeeper:3.6.3'
// Etcd is currently commented out
// implementation 'com.coreos:jetcd-core:0.0.1'
```

## Redis-Specific Parameters

| Parameter | Description |
|---|---|
| `-M` / `--master` | Sentinel master name |
| `-S` / `--sentinelPassword` | Sentinel password |
| `-k` / `--key` | Key to operate on |
| `-C` / `--cluster` | Cluster-related configuration |

## Redis Query Syntax Convention

`RedisTester.execute()` currently splits the command string by whitespace and supports:
- `GET <key>`
- `SET <key> <value>`
- `DEL <key>`
- `KEYS <pattern>`

Example:

```bash
java -jar build/libs/oneclient-1.0-all.jar \
  -h 127.0.0.1 -P 6379 -t query -e redis -q "GET mykey"
```

## Key Concerns

- `RedisTester` uses `JedisPool` with a default `setMaxTotal(800)`.
- `redis-cluster` has its own `RedisClusterTester`; connection logic differs from standalone/Sentinel.
- The Etcd dependency is currently commented out; enable it and implement the Tester before use.

## Maintenance Notes
- When adding a Redis command or protocol alias, sync `TesterFactory.java` and the `DBConfig.java` whitelist.
- When changing the Redis command set in `RedisTester`, sync this skill.
