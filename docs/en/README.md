# DBClient (OneClient) User Guide

## 1. Introduction

DBClient (OneClient) is a unified Java-based database client tool designed for accessing databases in KubeBlocks and other environments under various network conditions. This command-line tool supports connection testing, stress testing, benchmark testing, and query testing for 30+ database types.

## 2. Supported Database Types

### 2.1 Relational Databases
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
- PolarDB-X
- FoxLake
- GreatSQL
- GreatDB

### 2.2 NoSQL Databases
- MongoDB
- Redis (including Sentinel mode)

### 2.3 Search Engines
- Elasticsearch (v7, v8)
- OpenSearch

### 2.4 Time-Series Databases
- InfluxDB
- TDengine
- VictoriaMetrics
- VictoriaLogs
- Loki

### 2.5 Message Queues
- Kafka
- RabbitMQ
- RocketMQ
- Pulsar

### 2.6 Vector Databases
- Milvus
- Qdrant
- Nebula

### 2.7 Object Storage
- MinIO

### 2.8 Others
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

## 3. Supported Test Types

### 3.1 Query Test
Execute a single SQL query or database operation command and return the results.

**Use Cases**: Verify database connections, execute ad-hoc queries, check data status

### 3.2 Connection Stress Test
Connection stress test that creates a specified number of concurrent connections and maintains them for a period to test the database's connection handling capacity.

**Use Cases**: Test maximum connections, validate connection pool configuration, evaluate concurrent connection performance

**Key Parameters**: `--connections`, `--duration`

### 3.3 Benchmark Test
Benchmark test that executes queries multiple times at a specified concurrency level to evaluate database performance.

**Use Cases**: Performance baseline testing, query optimization validation, hardware comparison testing

**Key Parameters**: `--iterations`, `--concurrency`, `--query`

### 3.4 Execution Loop Test
Loop execution test that periodically executes queries within a specified duration with configurable intervals.

**Use Cases**: Long-term stability testing, monitor performance fluctuations, simulate real workloads

**Key Parameters**: `--duration`, `--interval`, `--query`

## 4. Build and Installation

### 4.1 Requirements
- Java 11 or higher
- Gradle 8.x or higher

### 4.2 Build Steps

```bash
# Clone the repository
git clone <repository-url>
cd DBClient

# Build the project
gradle build

# Generated JAR file location
./build/libs/oneclient-1.0-all.jar
```

### 4.3 Docker Build

```bash
# Multi-architecture build
docker buildx build -f docker/Dockerfile \
  -t docker.io/apecloud/dbclient:latest \
  --platform=linux/amd64,linux/arm64 \
  . --push

# Local build
docker build -f docker/Dockerfile -t apecloud/dbclient:latest .
```

## 5. Command Line Parameters

### 5.1 Required Parameters

| Parameter | Full Name | Description | Example |
|-----------|-----------|-------------|---------|
| -h | --host | Database host address | `127.0.0.1` |
| -u | --user | Database username | `postgres` |
| -p | --password | Database password | `mypassword` |
| -P | --port | Database port number | `5432` |
| -d | --database | Database name | `mydb` |
| -e | --dbtype | Database type | `postgresql` |

### 5.2 Basic Database Configuration Options

| Parameter | Full Name | Description | Default |
|-----------|-----------|-------------|---------|
| -o | --org | Organization name (required by some databases) | - |
| -tb | --table | Table name | - |
| -a | --accessmode | Access mode (mysql/postgresql/oracle/redis/influxdb/prometheus) | `mysql` |

### 5.3 Test Related Options

| Parameter | Full Name | Description | Default |
|-----------|-----------|-------------|---------|
| -t | --test | Test type (query/connectionstress/benchmark/executionloop) | - |
| -q | --query | SQL query or command to execute | - |
| -c | --connections | Number of connections | `100` |
| -s | --duration | Test duration (seconds) | `60` |
| -I | --interval | Intermediate statistics report interval (seconds), 0 disables | `1` |
| -i | --iterations | Benchmark test iteration count | - |
| -m | --concurrency | Benchmark test concurrency level | `10` |
| -M | --master | Redis sentinel master node | - |
| -S | --sentinelPassword | Redis sentinel password | - |
| -k | --key | Database key (for Redis, etc.) | - |
| -T | --topic | Database topic (for Kafka, etc.) | - |
| -B | --bucket | Database bucket (for MinIO, InfluxDB, etc.) | - |
| -C | --cluster | Database cluster name | - |

## 6. Usage Examples

### 6.1 MySQL Query Test

```bash
java -jar ./build/libs/oneclient-1.0-all.jar \
  -e mysql -h localhost -P 3306 -u root -p password \
  -d testdb -t query \
  -q "SELECT * FROM users LIMIT 10"
```

**Example Output**:
```
Test Result:
id	name	email	created_at
1	John	john@example.com	2024-01-01
2	Jane	jane@example.com	2024-01-02
...

Connection Information:
Database Type: mysql
Host: localhost
Port: 3306
Database: testdb
User: root
Test Type: query
Query: SELECT * FROM users LIMIT 10
```

### 6.2 PostgreSQL Connection Stress Test

```bash
java -jar ./build/libs/oneclient-1.0-all.jar \
  -e postgresql -h localhost -P 5432 -u postgres -p password \
  -t connectionstress -c 100 -s 60
```

**Example Output**:
```
Test Result:
null

Connection Information:
Database Type: postgresql
Host: localhost
Port: 5432
Database: postgres
User: postgres
Test Type: connectionstress
Connection Count: 100
Duration: 60 seconds
```

Or using Gradle:
```bash
gradle runOneclient --args="--password='password' --port=5432 --database=postgres \
  --user=postgres --dbtype=postgresql --test=connectionstress --connections=100"
```

### 6.3 Redis Benchmark Test

```bash
java -jar ./build/libs/oneclient-1.0-all.jar \
  -e redis -h localhost -P 6379 \
  -t benchmark -q "GET mykey" -i 1000 -m 10
```

**Example Output**:
```
Test Result:
Benchmark completed with 1000 iterations and 10 concurrency

Connection Information:
Database Type: redis
Host: localhost
Port: 6379
User: default
Test Type: benchmark
Iterations: 1000
Concurrency: 10
Query: GET mykey
```

### 6.4 MongoDB Loop Execution Test

```bash
java -jar ./build/libs/oneclient-1.0-all.jar \
  -e mongodb -h localhost -P 27017 -d testdb \
  -t executionloop -s 300 -I 10 \
  -q "db.collection.find({})"
```

**Example Output**:
```
Test Result:
Execution loop completed

Connection Information:
Database Type: mongodb
Host: localhost
Port: 27017
Database: testdb
Test Type: executionloop
Duration: 300 seconds
Interval: 10 seconds
Query: db.collection.find({})
```

### 6.5 ClickHouse Query Test

```bash
java -jar ./build/libs/oneclient-1.0-all.jar \
  -e clickhouse -h localhost -P 8123 -u default -p password \
  -d default -t query \
  -q "SELECT count(*) FROM system.tables"
```

### 6.6 Kafka Message Test

```bash
java -jar ./build/libs/oneclient-1.0-all.jar \
  -e kafka -h localhost -P 9092 \
  -t query -T my-topic \
  -q "SELECT * FROM topic"
```

### 6.7 MinIO Bucket Test

```bash
java -jar ./build/libs/oneclient-1.0-all.jar \
  -e minio -h localhost -P 9000 -u admin -p password \
  -t query -B my-bucket \
  -q "LIST objects"
```

### 6.8 Multiple Database Type Examples

**Oracle**: 
```bash
java -jar ./build/libs/oneclient-1.0-all.jar \
  -e oracle -h localhost -P 1521 -u system -p password \
  -d ORCLCDB -t query \
  -q "SELECT * FROM dual"
```

**Elasticsearch**:
```bash
java -jar ./build/libs/oneclient-1.0-all.jar \
  -e elasticsearch -h localhost -P 9200 \
  -t query -q "GET _cat/indices?v"
```

**TDengine**:
```bash
java -jar ./build/libs/oneclient-1.0-all.jar \
  -e tdengine -h localhost -P 6041 -u root -p password \
  -d log -t query \
  -q "SHOW DATABASES"
```

## 7. Network Access Solutions

### 7.1 Access Within Same Kubernetes Cluster

When applications are in the same Kubernetes cluster as KubeBlocks:

**Using ClusterIP**:
```bash
--host=<service-name>.<namespace>.svc.cluster.local
```

**Using Headless Service**:
```bash
--host=<pod-name>.<service-name>.<namespace>.svc.cluster.local
```

### 7.2 Cross-Cluster or External Environment Access

When applications are in different clusters or external environments:

**Using NodePort**:
```bash
--host=<node-ip> --port=<node-port>
```

**Using LoadBalancer**:
```bash
--host=<load-balancer-ip> --port=<lb-port>
```

### 7.3 Cross-Namespace Access Example

```bash
java -jar ./build/libs/oneclient-1.0-all.jar \
  -e postgresql \
  -h mydb.database.svc.cluster.local \
  -P 5432 -u admin -p secret \
  -d mydb -t query \
  -q "SELECT version()"
```

## 8. Kubernetes Deployment

### 8.1 Running as Pod

Create `dbclient-pod.yaml`:

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

Apply configuration:
```bash
kubectl apply -f dbclient-pod.yaml
```

View logs:
```bash
kubectl logs <pod-name>
```

## 9. Docker Usage

### 9.1 Running Container

```bash
# Basic query
docker run --rm apecloud/dbclient:latest \
  --host=127.0.0.1 --user=postgres --database=postgres \
  --password=mysecretpassword --port=5432 --dbtype=postgresql \
  --test=query --query="SELECT * FROM pg_user LIMIT 1"

# Connection stress test
docker run --rm apecloud/dbclient:latest \
  --host=127.0.0.1 --user=postgres --database=postgres \
  --password=mysecretpassword --port=5432 --dbtype=postgresql \
  --test=connectionstress --connections=10

# Benchmark test
docker run --rm apecloud/dbclient:latest \
  --host=127.0.0.1 --user=postgres --database=postgres \
  --password=mysecretpassword --port=5432 --dbtype=postgresql \
  --test=benchmark --query="SELECT * FROM users WHERE id = 1" \
  --iterations=1000 --concurrency=10
```

### 9.2 Using Host Network

```bash
docker run --rm --network host apecloud/dbclient:latest \
  --host=127.0.0.1 --user=postgres --password=secret \
  --database=test --port=5432 --dbtype=postgresql \
  --test=query --query="SELECT version()"
```

## 10. Test Results Explanation

After executing a test, the tool outputs the following information:

### 10.1 Test Result
- **query**: Query result data
- **connectionstress**: null (success) or error message
- **benchmark**: Completion statistics
- **executionloop**: Execution summary

### 10.2 Connection Information

Basic information:
- Database Type: Database type
- Host: Host address
- Port: Port number
- Database: Database name
- Table: Table name (if specified)
- User: Username
- Org: Organization name (if specified)
- Access Mode: Access mode
- Test Type: Test type

Additional information based on test type:

**connectionstress**:
- Connection Count: Number of connections
- Duration: Duration in seconds

**benchmark**:
- Iterations: Number of iterations
- Concurrency: Concurrency level
- Query: Query statement

**query**:
- Query: Query statement
- Cluster: Cluster name (for RocketMQ, etc.)

**executionloop**:
- Query: Query statement
- Duration: Duration in seconds
- Interval: Interval in seconds
- Key: Key name (for Redis)
- Topic: Topic name (for Kafka)
- Bucket: Bucket name (for MinIO, InfluxDB)
- Cluster: Cluster name (for ClickHouse, RocketMQ)

## 11. Database Type Values Reference

Use these values for the `--dbtype` parameter:

### MySQL Family
- `mysql`, `mysql 5.1`
- `tidb`, `mariadb`
- `polardbx`, `foxlake`
- `greatsql`, `greatdb`
- `greptime`
- `doris`, `selectdb`
- `starrocks`, `sr`

### PostgreSQL Family
- `postgresql`, `pg`, `postgres`
- `opentenbase`

### Other Relational Databases
- `oracle`
- `sqlserver`, `mssql`
- `oceanbase`, `ob`
- `gaussdb`
- `mogdb`
- `opengauss`
- `kingbase`
- `vastbase`
- `gbase8c`, `gbase`
- `dameng`, `dm`, `dmdb`, `damengdb`

### NoSQL
- `mongodb`, `mongo`
- `redis`, `sentinelredis`

### Search Engines
- `elasticsearch`, `es`, `elastic`
- `elasticsearch7`, `elasticsearch8`
- `opensearch`

### Time-Series Databases
- `influxdb`, `influx`
- `tdengine`, `td`, `taos`
- `victoriametrics`, `vm`, `victoria-metrics`
- `victorialogs`, `victoria-logs`
- `loki`

### Message Queues
- `kafka`
- `rabbitmq`
- `rocketmq`
- `pulsar`

### Vector Databases
- `milvus`
- `qdrant`
- `nebula`

### Others
- `clickhouse`, `ck`
- `zookeeper`, `zk`
- `etcd`
- `minio`
- `hadoop`
- `hive`
- `vault`

## 12. Notes and Best Practices

1. **Database Service**: Ensure the target database service is running and accessible
2. **Authentication**: Provide correct authentication information (username, password, etc.)
3. **Required Parameters**: Provide necessary parameters based on test type and database type (e.g., topic, bucket, cluster)
4. **Production Environment**: Some tests (especially connectionstress and benchmark) may generate heavy load on the database - use caution in production environments
5. **Driver Dependencies**: Some databases may require additional driver dependencies, which are included in the built JAR file
6. **Resource Limits**: Ensure sufficient system resources (memory, CPU) for test execution
7. **Network Connectivity**: Ensure network connectivity and configure firewall rules and security groups as needed
8. **Timeout Settings**: For long-running tests, set appropriate duration parameters

## 13. Troubleshooting

### Common Issues

#### 1. Connection Refused
**Causes**:
- Database service not started
- Incorrect host address or port
- Firewall blocking connection

**Solutions**:
```bash
# Check database service status
kubectl get pods -n <namespace>

# Test network connectivity
telnet <host> <port>

# Use host network (Docker)
docker run --rm --network host ...
```

#### 2. Authentication Failed
**Causes**:
- Incorrect username or password
- Insufficient user permissions
- Redis Sentinel configuration error

**Solutions**:
- Verify username and password
- Check user permissions
- For Redis Sentinel, ensure correct master name and password

#### 3. Unsupported Database Type
**Error Message**: `Unsupported database type: xxx`

**Solution**:
- Check if `--dbtype` parameter value is correct
- Confirm the database type is in the supported list

#### 4. Query Timeout
**Causes**:
- High query complexity
- High database load
- Network latency

**Solutions**:
- Optimize query statement
- Check database load
- Increase timeout settings (if supported)

#### 5. Insufficient Resources
**Error Message**: `OutOfMemoryError` or other resource-related errors

**Solutions**:
```bash
# Increase JVM heap memory
java -Xmx2g -jar ./build/libs/oneclient-1.0-all.jar ...

# Reduce concurrency or connections
--connections=50 --concurrency=5
```

#### 6. Driver Loading Failure
**Error Message**: `ClassNotFoundException`

**Solution**:
- Ensure using the fully built JAR file (oneclient-1.0-all.jar)
- Check if build.gradle includes the driver dependency for the corresponding database

### Debugging Tips

1. **Enable Detailed Logging**:
   ```bash
   # Modify logback.xml configuration, set log level to DEBUG
   ```

2. **Test with Minimal Configuration**:
   Start with the simplest parameters and gradually add complex ones

3. **View Complete Error Stack**:
   Don't just look at the error message; review the complete exception stack trace

4. **Use Simple Queries**:
   First verify the connection with simple queries like `SELECT 1` or `SELECT version()`

## 14. Advanced Usage

### 14.1 Custom Access Modes

Some databases support multiple access modes:

```bash
# MySQL mode
java -jar ... -a mysql ...

# PostgreSQL mode
java -jar ... -a postgresql ...

# Oracle mode
java -jar ... -a oracle ...
```

### 14.2 Redis Sentinel Mode

```bash
java -jar ./build/libs/oneclient-1.0-all.jar \
  -e redis -h sentinel-host -P 26379 \
  -M mymaster -S sentinel-password \
  -t query -q "PING"
```

### 14.3 Batch Testing Multiple Databases

Write a script to batch test multiple databases:

```bash
#!/bin/bash
for db in mysql postgresql mongodb; do
  echo "Testing $db..."
  java -jar ./build/libs/oneclient-1.0-all.jar \
    -e $db -h localhost -P ${PORTS[$db]} \
    -u ${USERS[$db]} -p ${PASSWORDS[$db]} \
    -t query -q "SELECT 1"
done
```

### 14.4 Performance Monitoring

Combine with other tools for performance monitoring:

```bash
# Use watch command to execute periodically
watch -n 10 'java -jar ... -t query -q "SELECT count(*) FROM users"'

# Monitor system resources with top/htop
top & java -jar ... -t connectionstress -c 100
```

## 15. Development Guide

### 15.1 Project Structure

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
└── docs/                           # Documentation
```

### 15.2 Adding New Database Support

1. Create a new Tester class in `src/main/java/com/apecloud/dbtester/tester/`
2. Register the new tester in `TesterFactory.java`
3. Add database driver or client library dependency in `build.gradle`
4. Add recognition for the new database type in `DBConfig.java`
5. Add connection logic in `DatabaseConnectionFactory.java`
6. Update this documentation

## 16. More Information

- Main README: [../README.md](../README.md)
- Docker Usage: [../../docker/README.md](../../docker/README.md)
- Project Source: Check the root directory of the repository

## 17. License

This project is licensed under the Apache License 2.0.

## 18. Contributing

Contributions are welcome! Please feel free to submit issues and pull requests. For questions or suggestions, please provide feedback through:
- Submitting an Issue
- Creating a Pull Request