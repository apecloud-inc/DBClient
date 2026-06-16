---
name: dbclient-engine-mq
description: >
  Message-queue engines in DBClient: Kafka, Pulsar, RabbitMQ, and RocketMQ.
  Use this skill when adding, debugging, or modifying such a Tester.
---

# DBClient Engine: Message Queue

## Covered Testers and Aliases

| Engine | Tester | Aliases |
|---|---|---|
| Kafka | `KafkaTester.java` | `kafka` |
| Pulsar | `PulsarTester.java` | `pulsar` |
| RabbitMQ | `RabbitMQTester.java` | `rabbitmq` |
| RocketMQ | `RocketMQTester.java` | `rocketmq` |

## Key Dependencies (`build.gradle`)

```groovy
// Kafka
implementation 'org.apache.kafka:kafka-clients:2.8.0'

// Pulsar
implementation 'org.apache.pulsar:pulsar-client:2.10.0'
implementation 'org.apache.pulsar:pulsar-client-admin:2.10.0'

// RabbitMQ
implementation 'com.rabbitmq:amqp-client:5.23.0'

// RocketMQ
implementation 'org.apache.rocketmq:rocketmq-client:4.9.8'
implementation 'org.apache.rocketmq:rocketmq-tools:4.9.8'
```

## Specialized Parameters

| Parameter | Purpose |
|---|---|
| `-T` / `--topic` | Topic for Kafka / Pulsar / RocketMQ |
| `-k` / `--key` | Message key |
| `-C` / `--cluster` | Cluster name for RocketMQ, etc. |
| `-B` / `--bucket` | Used mainly for object storage; rarely used for MQ |

## Key Concerns

- The `execute()` of an MQ Tester usually means "produce/consume one message", not SQL semantics; the query parameter format is defined by each Tester.
- For stress and loop tests, watch client connection overhead, topic metadata fetch, and consumer-group rebalances.
- Kafka 2.8.0 compatibility with newer brokers needs attention for KRaft / newer protocol versions.

## Minimal Verification Command (Kafka Example)

```bash
java -jar build/libs/oneclient-1.0-all.jar \
  -h 127.0.0.1 -P 9092 -e kafka -t query \
  -T my-topic -q "test-message"
```

## Maintenance Notes
- When upgrading an MQ client version, sync the dependency coordinates in this skill.
- When adding a new MQ engine, `KafkaTester` is a good reference template.
