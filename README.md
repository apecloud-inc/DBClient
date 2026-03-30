# DBClient

A unified database client tool for accessing various databases in KubeBlocks and other environments. DBClient provides a comprehensive command-line interface named **OneClient** that supports basic operations and advanced testing capabilities including connection stress tests, benchmarks, and query execution.

## Features

- **30+ Database Support**: Supports a wide range of databases including MySQL, PostgreSQL, Oracle, MongoDB, Redis, Kafka, Elasticsearch, ClickHouse, and many more
- **Multiple Test Types**: Query execution, connection stress testing, benchmarking, and execution loops
- **Flexible Network Access**: Works with ClusterIP, NodePort, LoadBalancer, and headless services in Kubernetes
- **Unified Interface**: Single command-line tool for all supported databases
- **Advanced Testing**: Connection pool stress tests, concurrent query execution, and performance benchmarking

## Supported Databases

### Relational Databases
- MySQL (including MySQL 5.1)
- PostgreSQL
- Oracle
- SQL Server
- MariaDB
- TiDB
- OceanBase
- GaussDB
- MogDB
- OpenGauss
- KingBase
- Dameng (DM)
- Vastbase
- GBase 8c
- YashanDB
- PolarDB-X
- FoxLake
- GreatSQL
- GreatDB

### NoSQL Databases
- MongoDB
- Redis (including Sentinel mode)
- Cassandra
- HBase

### Search Engines
- Elasticsearch (v7, v8)
- OpenSearch
- Solr

### Time-Series Databases
- InfluxDB
- TDengine
- VictoriaMetrics
- VictoriaLogs
- Loki

### Message Queues
- Kafka
- RabbitMQ
- RocketMQ
- Pulsar

### Vector Databases
- Milvus
- Qdrant
- Nebula

### Object Storage
- MinIO

### Others
- ZooKeeper
- Etcd
- ClickHouse
- Doris
- StarRocks
- SelectDB
- GreptimeDB
- Hadoop
- Hive
- Vault

## Installation

### Build from Source

```bash
# Clone the repository
git clone <repository-url>
cd DBClient

# Build the project
gradle build

# The built JAR file will be available at:
./build/libs/oneclient-1.0-all.jar
```

### Requirements

- Java 11 or higher
- Gradle 8.x or higher

## Usage

### Basic Command Structure

```bash
java -jar ./build/libs/oneclient-1.0-all.jar [OPTIONS]
```

### Required Parameters

| Parameter | Short | Description | Example |
|-----------|-------|-------------|---------|
| `--host` | `-h` | Database host | `127.0.0.1` |
| `--user` | `-u` | Database username | `postgres` |
| `--password` | `-p` | Database password | `mypassword` |
| `--port` | `-P` | Database port | `5432` |
| `--database` | `-d` | Database name | `mydb` |
| `--dbtype` | `-e` | Database type | `postgresql` |

### Optional Parameters

| Parameter | Short | Description | Default |
|-----------|-------|-------------|---------|
| `--test` | `-t` | Test type (query/connectionstress/benchmark/executionloop) | - |
| `--query` | `-q` | SQL query to execute | - |
| `--table` | `-tb` | Table name | - |
| `--connections` | `-c` | Number of connections | `100` |
| `--duration` | `-s` | Test duration in seconds | `60` |
| `--interval` | `-I` | Statistics report interval (seconds) | `1` |
| `--iterations` | `-i` | Number of iterations for benchmark | - |
| `--concurrency` | `-m` | Concurrency level | `10` |
| `--accessmode` | `-a` | Access mode | `mysql` |
| `--org` | `-o` | Organization name | - |
| `--master` | `-M` | Redis sentinel master | - |
| `--sentinelPassword` | `-S` | Redis sentinel password | - |
| `--key` | `-k` | Key (for Redis, etc.) | - |
| `--topic` | `-T` | Topic (for Kafka, etc.) | - |
| `--bucket` | `-B` | Bucket (for MinIO, InfluxDB, etc.) | - |
| `--cluster` | `-C` | Cluster name | - |

## Examples

### 1. Run a Simple Query

Execute a query on PostgreSQL:

```bash
java -jar ./build/libs/oneclient-1.0-all.jar \
  --host=127.0.0.1 \
  --user=postgres \
  --database=postgres \
  --password=mysecretpassword \
  --test=query \
  --query="SELECT * FROM pg_user LIMIT 1" \
  --port=5432 \
  --dbtype=postgresql
```

**Example Output:**
```
SLF4J(I): Connected with provider of type [ch.qos.logback.classic.spi.LogbackServiceProvider]
Test Result:
usename	usesysid	usecreatedb	usesuper	userepl	usebypassrls	passwd	valuntil	useconfig
postgres	10	t	t	t	t	********	null	null

Connection Information:
Database Type: postgresql
Host: 127.0.0.1
Port: 5432
Database: postgres
Table: user
User: postgres
Test Type: query
Query: SELECT * FROM pg_user LIMIT 1
```

### 2. Connection Stress Test

Test maximum connections with PostgreSQL:

```bash
java -jar ./build/libs/oneclient-1.0-all.jar \
  --host=127.0.0.1 \
  --user=postgres \
  --database=postgres \
  --password=mysecretpassword \
  --test=connectionstress \
  --connections=10 \
  --port=5432 \
  --dbtype=postgresql
```

**Example Output:**
```
SLF4J(I): Connected with provider of type [ch.qos.logback.classic.spi.LogbackServiceProvider]
Test Result:
null

Connection Information:
Database Type: postgresql
Host: 127.0.0.1
Port: 5432
Database: postgres
Table: user
User: postgres
Test Type: connectionstress
Connection Count: 10
Duration: 60 seconds
```

Or using Gradle:

```bash
gradle runOneclient --args="--password='mysecretpassword' --port=5432 --database=postgres --user=postgres --dbtype=postgresql --test=connectionstress --connections=10"
```

### 3. Run Benchmark Test

Execute benchmark with concurrent queries:

```bash
java -jar ./build/libs/oneclient-1.0-all.jar \
  --host=127.0.0.1 \
  --user=postgres \
  --database=postgres \
  --password=mysecretpassword \
  --test=benchmark \
  --query="SELECT * FROM users WHERE id = 1" \
  --iterations=1000 \
  --concurrency=10 \
  --port=5432 \
  --dbtype=postgresql
```

**Example Output:**
```
SLF4J(I): Connected with provider of type [ch.qos.logback.classic.spi.LogbackServiceProvider]
Test Result:
Benchmark completed with 1000 iterations and 10 concurrency

Connection Information:
Database Type: postgresql
Host: 127.0.0.1
Port: 5432
Database: postgres
Table: user
User: postgres
Test Type: benchmark
Iterations: 1000
Concurrency: 10
Query: SELECT * FROM users WHERE id = 1
```

### 4. MySQL Query Example

```bash
java -jar ./build/libs/oneclient-1.0-all.jar \
  --host=127.0.0.1 \
  --user=root \
  --database=testdb \
  --password=mysqlpassword \
  --test=query \
  --query="SHOW DATABASES" \
  --port=3306 \
  --dbtype=mysql
```

### 5. MongoDB Query Example

```bash
java -jar ./build/libs/oneclient-1.0-all.jar \
  --host=127.0.0.1 \
  --user=admin \
  --database=testdb \
  --password=mongopassword \
  --test=query \
  --query="db.collection.find()" \
  --port=27017 \
  --dbtype=mongodb
```

### 6. Redis Query Example

```bash
java -jar ./build/libs/oneclient-1.0-all.jar \
  --host=127.0.0.1 \
  --password=redispassword \
  --test=query \
  --query="GET mykey" \
  --port=6379 \
  --dbtype=redis
```

### 7. Execution Loop Test

Continuously execute queries with specified duration and interval:

```bash
java -jar ./build/libs/oneclient-1.0-all.jar \
  --host=127.0.0.1 \
  --user=postgres \
  --database=postgres \
  --password=mysecretpassword \
  --test=executionloop \
  --query="SELECT COUNT(*) FROM users" \
  --duration=300 \
  --interval=10 \
  --port=5432 \
  --dbtype=postgresql
```

## Running in Kubernetes

Deploy DBClient as a Kubernetes Pod:

```yaml
apiVersion: v1
kind: Pod
metadata:
  generateName: test-dbclient-
  namespace: default
spec:
  containers:
    - name: test-dbclient
      imagePullPolicy: IfNotPresent
      image: docker.io/apecloud/dbclient:latest
      args:
        - "--host"
        - "127.0.0.1"
        - "--user"
        - "postgres"
        - "--database"
        - "postgres"
        - "--port"
        - "5432"
        - "--password"
        - "mysecretpassword"
        - "--test"
        - "query"
        - "--query"
        - "SELECT * FROM pg_user LIMIT 1"
        - "--dbtype"
        - "postgresql"
  restartPolicy: Never
```

Apply the configuration:

```bash
kubectl apply -f dbclient-pod.yaml
```

## Docker Usage

Build and run using Docker:

```bash
# Build the Docker image
docker build -t apecloud/dbclient:latest .

# Run the container
docker run --rm apecloud/dbclient:latest \
  --host=127.0.0.1 \
  --user=postgres \
  --database=postgres \
  --password=mysecretpassword \
  --test=query \
  --query="SELECT * FROM pg_user LIMIT 1" \
  --port=5432 \
  --dbtype=postgresql
```

## Test Types

### Query Test (`query`)
Execute a single query and return the results.

### Connection Stress Test (`connectionstress`)
Test the database's ability to handle multiple concurrent connections.
- **Parameters**: `--connections`, `--duration`

### Benchmark Test (`benchmark`)
Measure query performance under concurrent load.
- **Parameters**: `--iterations`, `--concurrency`, `--query`

### Execution Loop Test (`executionloop`)
Continuously execute queries for a specified duration with configurable intervals.
- **Parameters**: `--duration`, `--interval`, `--query`

## Database Type Values

Use these values for the `--dbtype` parameter:

- **MySQL Family**: `mysql`, `mysql 5.1`, `tidb`, `mariadb`, `polardbx`, `foxlake`, `greatsql`, `greatdb`, `greptime`, `doris`, `selectdb`, `starrocks`, `sr`
- **PostgreSQL Family**: `postgresql`, `pg`, `postgres`, `opentenbase`
- **Oracle**: `oracle`
- **SQL Server**: `sqlserver`, `mssql`
- **MongoDB**: `mongodb`, `mongo`
- **Redis**: `redis`, `sentinelredis`
- **Elasticsearch**: `elasticsearch`, `elasticsearch7`, `elasticsearch8`, `es`, `elastic`
- **OpenSearch**: `opensearch`
- **ClickHouse**: `clickhouse`, `ck`
- **Dameng**: `dameng`, `dm`, `dmdb`, `damengdb`
- **InfluxDB**: `influxdb`, `influx`
- **TDengine**: `tdengine`, `td`, `taos`
- **VictoriaMetrics**: `victoriametrics`, `vm`, `victoria-metrics`
- **VictoriaLogs**: `victorialogs`, `victoria-logs`
- **Kafka**: `kafka`
- **RabbitMQ**: `rabbitmq`
- **RocketMQ**: `rocketmq`
- **Pulsar**: `pulsar`
- **ZooKeeper**: `zookeeper`, `zk`
- **Etcd**: `etcd`
- **MinIO**: `minio`
- **Milvus**: `milvus`
- **Qdrant**: `qdrant`
- **Nebula**: `nebula`
- **GaussDB**: `gaussdb`
- **OpenGauss**: `opengauss`
- **MogDB**: `mogdb`
- **KingBase**: `kingbase`
- **Vastbase**: `vastbase`
- **GBase8c**: `gbase8c`, `gbase`
- **Hadoop**: `hadoop`
- **Hive**: `hive`
- **Loki**: `loki`
- **Vault**: `vault`

## Network Solutions

### Same Kubernetes Cluster
When your applications run in the same Kubernetes cluster as KubeBlocks:
- Use **ClusterIP** for internal access
- Use **headless services** for direct pod access
- Use service DNS names for discovery

### Cross-Cluster or External Access
When accessing databases from different clusters or external environments:
- Use **NodePort** for simple external access
- Use **LoadBalancer** for production-grade access
- Configure proper firewall rules and security groups

## Troubleshooting

### Common Issues

1. **Connection Refused**
   - Verify the database is running
   - Check host and port settings
   - Ensure network connectivity

2. **Authentication Failed**
   - Verify username and password
   - Check user permissions
   - For Redis Sentinel, ensure correct master name and password

3. **Driver Not Found**
   - Ensure all dependencies are included in the JAR
   - Check if the database type is supported

4. **Query Timeout**
   - Increase timeout settings if applicable
   - Optimize query performance
   - Check database load

## Development

### Project Structure

```
DBClient/
├── src/main/java/
│   ├── OneClient.java              # Main entry point
│   └── com/apecloud/dbtester/
│       ├── commons/                # Common utilities and interfaces
│       │   ├── DBConfig.java
│       │   ├── DatabaseTester.java
│       │   ├── TesterFactory.java
│       │   └── ...
│       └── tester/                 # Database-specific testers
│           ├── MySQLTester.java
│           ├── PostgreSQLTester.java
│           └── ...
├── build.gradle                    # Build configuration
├── docker/                         # Docker configuration
└── README.md                       # This file
```

### Adding New Database Support

To add support for a new database:

1. Create a new Tester class in `src/main/java/com/apecloud/dbtester/tester/`
2. Register the tester in `TesterFactory.java`
3. Add JDBC driver or client library to `build.gradle`
4. Update `DBConfig.java` to recognize the new database type
5. Update `DatabaseConnectionFactory.java` with connection logic

## License

This project is licensed under the Apache License 2.0.

## Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues for bug reports and feature requests.