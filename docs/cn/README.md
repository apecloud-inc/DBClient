# DBClient 测试工具使用说明书

## 1. 简介

DBClient（OneClient）是一个基于 Java 开发的统一数据库客户端工具，支持在多种网络环境下访问 KubeBlocks 中的数据库。该工具通过命令行参数配置，可以对 30+ 种数据库执行连接测试、压力测试、基准测试和查询测试。

## 2. 支持的数据库类型

### 2.1 关系型数据库
- MySQL（包括 MySQL 5.1）
- PostgreSQL
- Oracle
- SQL Server
- MariaDB
- TiDB
- OceanBase
- GaussDB
- MogDB
- OpenGauss
- KingBase（人大金仓）
- Dameng（达梦）
- Vastbase（海量）
- GBase 8c
- PolarDB-X
- FoxLake
- GreatSQL
- GreatDB

### 2.2 NoSQL 数据库
- MongoDB
- Redis（包括 Sentinel 模式）

### 2.3 搜索引擎
- Elasticsearch（v7, v8）
- OpenSearch

### 2.4 时序数据库
- InfluxDB
- TDengine
- VictoriaMetrics
- VictoriaLogs
- Loki

### 2.5 消息队列
- Kafka
- RabbitMQ
- RocketMQ
- Pulsar

### 2.6 向量数据库
- Milvus
- Qdrant
- Nebula

### 2.7 对象存储
- MinIO

### 2.8 其他
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

## 3. 支持的测试类型

### 3.1 query 测试
执行单条 SQL 查询或数据库操作命令，返回查询结果。

**适用场景**：验证数据库连接、执行临时查询、检查数据状态

### 3.2 connectionstress 测试
连接压力测试，创建指定数量的并发连接并维持一段时间，测试数据库的连接处理能力。

**适用场景**：测试最大连接数、验证连接池配置、评估并发连接性能

**关键参数**：`--connections`（连接数）、`--duration`（持续时间）

### 3.3 benchmark 测试
基准测试，以指定的并发级别执行查询多次，评估数据库性能。

**适用场景**：性能基准测试、查询优化验证、硬件对比测试

**关键参数**：`--iterations`（迭代次数）、`--concurrency`（并发数）、`--query`（查询语句）

### 3.4 executionloop 测试
循环执行测试，在指定的持续时间内周期性执行查询，可配置间隔时间。

**适用场景**：长时间稳定性测试、监控性能波动、模拟真实负载

**关键参数**：`--duration`（持续时间）、`--interval`（间隔时间）、`--query`（查询语句）

## 4. 构建和安装

### 4.1 环境要求
- Java 11 或更高版本
- Gradle 8.x 或更高版本

### 4.2 构建步骤

```bash
# 克隆仓库
git clone <repository-url>
cd DBClient

# 构建项目
gradle build

# 生成的 JAR 文件位置
./build/libs/oneclient-1.0-all.jar
```

### 4.3 Docker 构建

```bash
# 多架构构建
docker buildx build -f docker/Dockerfile \
  -t docker.io/apecloud/dbclient:latest \
  --platform=linux/amd64,linux/arm64 \
  . --push

# 本地构建
docker build -f docker/Dockerfile -t apecloud/dbclient:latest .
```

## 5. 命令行参数说明

### 5.1 必需参数

| 参数 | 全称 | 说明 | 示例 |
|------|------|------|------|
| -h | --host | 数据库主机地址 | `127.0.0.1` |
| -u | --user | 数据库用户名 | `postgres` |
| -p | --password | 数据库密码 | `mypassword` |
| -P | --port | 数据库端口号 | `5432` |
| -d | --database | 数据库名称 | `mydb` |
| -e | --dbtype | 数据库类型 | `postgresql` |

### 5.2 基本数据库配置选项

| 参数 | 全称 | 说明 | 默认值 |
|------|------|------|------|
| -o | --org | 组织名称（部分数据库需要） | - |
| -tb | --table | 表名 | - |
| -a | --accessmode | 访问模式 (mysql/postgresql/oracle/redis/influxdb/prometheus) | `mysql` |

### 5.3 测试相关选项

| 参数 | 全称 | 说明 | 默认值 |
|------|------|------|------|
| -t | --test | 测试类型 (query/connectionstress/benchmark/executionloop) | - |
| -q | --query | 要执行的 SQL 查询或命令 | - |
| -c | --connections | 连接数 | `100` |
| -s | --duration | 测试持续时间（秒） | `60` |
| -I | --interval | 中间统计报告间隔（秒），0 禁用 | `1` |
| -i | --iterations | 基准测试迭代次数 | - |
| -m | --concurrency | 基准测试并发级别 | `10` |
| -M | --master | Redis 哨兵主节点 | - |
| -S | --sentinelPassword | Redis 哨兵密码 | - |
| -k | --key | 数据库键（Redis 等） | - |
| -T | --topic | 数据库主题（Kafka 等） | - |
| -B | --bucket | 数据库存储桶（MinIO、InfluxDB 等） | - |
| -C | --cluster | 数据库集群名称 | - |

## 6. 使用示例

### 6.1 MySQL 查询测试

```bash
java -jar ./build/libs/oneclient-1.0-all.jar \
  -e mysql -h localhost -P 3306 -u root -p password \
  -d testdb -t query \
  -q "SELECT * FROM users LIMIT 10"
```

**输出示例**：
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

### 6.2 PostgreSQL 连接压力测试

```bash
java -jar ./build/libs/oneclient-1.0-all.jar \
  -e postgresql -h localhost -P 5432 -u postgres -p password \
  -t connectionstress -c 100 -s 60
```

**输出示例**：
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

或使用 Gradle：
```bash
gradle runOneclient --args="--password='password' --port=5432 --database=postgres \
  --user=postgres --dbtype=postgresql --test=connectionstress --connections=100"
```

### 6.3 Redis 基准测试

```bash
java -jar ./build/libs/oneclient-1.0-all.jar \
  -e redis -h localhost -P 6379 \
  -t benchmark -q "GET mykey" -i 1000 -m 10
```

**输出示例**：
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

### 6.4 MongoDB 循环执行测试

```bash
java -jar ./build/libs/oneclient-1.0-all.jar \
  -e mongodb -h localhost -P 27017 -d testdb \
  -t executionloop -s 300 -I 10 \
  -q "db.collection.find({})"
```

**输出示例**：
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

### 6.5 ClickHouse 查询测试

```bash
java -jar ./build/libs/oneclient-1.0-all.jar \
  -e clickhouse -h localhost -P 8123 -u default -p password \
  -d default -t query \
  -q "SELECT count(*) FROM system.tables"
```

### 6.6 Kafka 消息测试

```bash
java -jar ./build/libs/oneclient-1.0-all.jar \
  -e kafka -h localhost -P 9092 \
  -t query -T my-topic \
  -q "SELECT * FROM topic"
```

### 6.7 MinIO 存储桶测试

```bash
java -jar ./build/libs/oneclient-1.0-all.jar \
  -e minio -h localhost -P 9000 -u admin -p password \
  -t query -B my-bucket \
  -q "LIST objects"
```

### 6.8 多数据库类型示例

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

## 7. 网络访问方案

### 7.1 同 Kubernetes 集群内访问

当应用与 KubeBlocks 在同一 Kubernetes 集群时：

**使用 ClusterIP**：
```bash
--host=<service-name>.<namespace>.svc.cluster.local
```

**使用 Headless Service**：
```bash
--host=<pod-name>.<service-name>.<namespace>.svc.cluster.local
```

### 7.2 跨集群或外部环境访问

当应用在不同集群或外部环境时：

**使用 NodePort**：
```bash
--host=<node-ip> --port=<node-port>
```

**使用 LoadBalancer**：
```bash
--host=<load-balancer-ip> --port=<lb-port>
```

### 7.3 跨命名空间访问示例

```bash
java -jar ./build/libs/oneclient-1.0-all.jar \
  -e postgresql \
  -h mydb.database.svc.cluster.local \
  -P 5432 -u admin -p secret \
  -d mydb -t query \
  -q "SELECT version()"
```

## 8. Kubernetes 部署

### 8.1 Pod 方式运行

创建 `dbclient-pod.yaml`：

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

应用配置：
```bash
kubectl apply -f dbclient-pod.yaml
```

查看日志：
```bash
kubectl logs <pod-name>
```

## 9. Docker 使用

### 9.1 运行容器

```bash
# 基本查询
docker run --rm apecloud/dbclient:latest \
  --host=127.0.0.1 --user=postgres --database=postgres \
  --password=mysecretpassword --port=5432 --dbtype=postgresql \
  --test=query --query="SELECT * FROM pg_user LIMIT 1"

# 连接压力测试
docker run --rm apecloud/dbclient:latest \
  --host=127.0.0.1 --user=postgres --database=postgres \
  --password=mysecretpassword --port=5432 --dbtype=postgresql \
  --test=connectionstress --connections=10

# 基准测试
docker run --rm apecloud/dbclient:latest \
  --host=127.0.0.1 --user=postgres --database=postgres \
  --password=mysecretpassword --port=5432 --dbtype=postgresql \
  --test=benchmark --query="SELECT * FROM users WHERE id = 1" \
  --iterations=1000 --concurrency=10
```

### 9.2 使用 Host 网络

```bash
docker run --rm --network host apecloud/dbclient:latest \
  --host=127.0.0.1 --user=postgres --password=secret \
  --database=test --port=5432 --dbtype=postgresql \
  --test=query --query="SELECT version()"
```

## 10. 测试结果说明

执行测试后，工具会输出以下信息：

### 10.1 Test Result（测试结果）
- **query**: 查询结果数据
- **connectionstress**: null（成功）或错误信息
- **benchmark**: 完成情况统计
- **executionloop**: 执行情况总结

### 10.2 Connection Information（连接信息）

基础信息：
- Database Type: 数据库类型
- Host: 主机地址
- Port: 端口号
- Database: 数据库名
- Table: 表名（如指定）
- User: 用户名
- Org: 组织名（如指定）
- Access Mode: 访问模式
- Test Type: 测试类型

根据测试类型的额外信息：

**connectionstress**:
- Connection Count: 连接数
- Duration: 持续时间（秒）

**benchmark**:
- Iterations: 迭代次数
- Concurrency: 并发数
- Query: 查询语句

**query**:
- Query: 查询语句
- Cluster: 集群名（RocketMQ 等）

**executionloop**:
- Query: 查询语句
- Duration: 持续时间（秒）
- Interval: 间隔时间（秒）
- Key: 键名（Redis）
- Topic: 主题（Kafka）
- Bucket: 存储桶（MinIO、InfluxDB）
- Cluster: 集群名（ClickHouse、RocketMQ）

## 11. 数据库类型值参考

使用 `--dbtype` 参数时，请使用以下值：

### MySQL 系列
- `mysql`, `mysql 5.1`
- `tidb`, `mariadb`
- `polardbx`, `foxlake`
- `greatsql`, `greatdb`
- `greptime`
- `doris`, `selectdb`
- `starrocks`, `sr`

### PostgreSQL 系列
- `postgresql`, `pg`, `postgres`
- `opentenbase`

### 其他关系型数据库
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

### 搜索引擎
- `elasticsearch`, `es`, `elastic`
- `elasticsearch7`, `elasticsearch8`
- `opensearch`

### 时序数据库
- `influxdb`, `influx`
- `tdengine`, `td`, `taos`
- `victoriametrics`, `vm`, `victoria-metrics`
- `victorialogs`, `victoria-logs`
- `loki`

### 消息队列
- `kafka`
- `rabbitmq`
- `rocketmq`
- `pulsar`

### 向量数据库
- `milvus`
- `qdrant`
- `nebula`

### 其他
- `clickhouse`, `ck`
- `zookeeper`, `zk`
- `etcd`
- `minio`
- `hadoop`
- `hive`
- `vault`

## 12. 注意事项

1. **数据库服务**：确保目标数据库服务正在运行且可访问
2. **认证信息**：提供正确的用户名、密码等认证信息
3. **必要参数**：根据测试类型和数据库类型提供必要的参数（如 topic、bucket、cluster 等）
4. **生产环境**：某些测试（特别是 connectionstress 和 benchmark）可能会对数据库产生大量负载，请在生产环境中谨慎使用
5. **驱动依赖**：部分数据库可能需要额外的驱动程序依赖，已包含在构建的 JAR 文件中
6. **资源限制**：确保有足够的系统资源（内存、CPU）执行测试
7. **网络连接**：确保网络连通性，必要时配置防火墙规则和安全组
8. **超时设置**：对于长时间运行的测试，注意设置合适的 duration 参数

## 13. 故障排除

### 常见问题

#### 1. 连接被拒绝（Connection Refused）
**原因**：
- 数据库服务未启动
- 主机地址或端口错误
- 防火墙阻止连接

**解决方法**：
```bash
# 检查数据库服务状态
kubectl get pods -n <namespace>

# 测试网络连通性
telnet <host> <port>

# 使用 host 网络（Docker）
docker run --rm --network host ...
```

#### 2. 认证失败（Authentication Failed）
**原因**：
- 用户名或密码错误
- 用户权限不足
- Redis Sentinel 配置错误

**解决方法**：
- 验证用户名和密码
- 检查用户权限
- 对于 Redis Sentinel，确保正确的主节点名称和密码

#### 3. 不支持的数据库类型
**错误信息**：`Unsupported database type: xxx`

**解决方法**：
- 检查 `--dbtype` 参数值是否正确
- 确认数据库类型在支持列表中

#### 4. 查询超时
**原因**：
- 查询复杂度高
- 数据库负载高
- 网络延迟

**解决方法**：
- 优化查询语句
- 检查数据库负载情况
- 增加超时设置（如果支持）

#### 5. 资源不足
**错误信息**：`OutOfMemoryError` 或其他资源相关错误

**解决方法**：
```bash
# 增加 JVM 堆内存
java -Xmx2g -jar ./build/libs/oneclient-1.0-all.jar ...

# 减少并发数或连接数
--connections=50 --concurrency=5
```

#### 6. 驱动加载失败
**错误信息**：`ClassNotFoundException`

**解决方法**：
- 确保使用完整构建的 JAR 文件（oneclient-1.0-all.jar）
- 检查 build.gradle 中是否包含对应数据库的驱动依赖

### 调试技巧

1. **启用详细日志**：
   ```bash
   # 修改 logback.xml 配置，设置日志级别为 DEBUG
   ```

2. **测试最小配置**：
   先用最简单的参数测试，逐步添加复杂参数

3. **查看完整错误栈**：
   不要只看错误信息，查看完整的异常堆栈

4. **使用简单查询**：
   先用 `SELECT 1` 或 `SELECT version()` 等简单查询验证连接

## 14. 高级用法

### 14.1 自定义访问模式

某些数据库支持多种访问模式：

```bash
# MySQL 模式
java -jar ... -a mysql ...

# PostgreSQL 模式
java -jar ... -a postgresql ...

# Oracle 模式
java -jar ... -a oracle ...
```

### 14.2 Redis Sentinel 模式

```bash
java -jar ./build/libs/oneclient-1.0-all.jar \
  -e redis -h sentinel-host -P 26379 \
  -M mymaster -S sentinel-password \
  -t query -q "PING"
```

### 14.3 批量测试多个数据库

编写脚本批量测试：

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

### 14.4 性能监控

结合其他工具进行性能监控：

```bash
# 使用 watch 命令定期执行
watch -n 10 'java -jar ... -t query -q "SELECT count(*) FROM users"'

# 结合 top/htop 监控系统资源
top & java -jar ... -t connectionstress -c 100
```

## 15. 开发指南

### 15.1 项目结构

```
DBClient/
├── src/main/java/
│   ├── OneClient.java              # 主入口
│   └── com/apecloud/dbtester/
│       ├── commons/                # 通用工具和接口
│       │   ├── DBConfig.java
│       │   ├── DatabaseTester.java
│       │   ├── TesterFactory.java
│       │   └── ...
│       └── tester/                 # 数据库特定测试器
│           ├── MySQLTester.java
│           ├── PostgreSQLTester.java
│           └── ...
├── build.gradle                    # 构建配置
├── docker/                         # Docker 配置
└── docs/                           # 文档
```

### 15.2 添加新数据库支持

1. 在 `src/main/java/com/apecloud/dbtester/tester/` 创建新的 Tester 类
2. 在 `TesterFactory.java` 中注册新的 tester
3. 在 `build.gradle` 中添加数据库驱动或客户端库依赖
4. 在 `DBConfig.java` 中添加对新数据库类型的识别
5. 在 `DatabaseConnectionFactory.java` 中添加连接逻辑
6. 更新本文档

## 16. 更多信息

- 主 README 文档：[../README.md](../README.md)
- Docker 使用说明：[../../docker/README.md](../../docker/README.md)
- 项目源码：查看仓库根目录

## 17. 许可证

本项目采用 Apache License 2.0 许可证。

## 18. 贡献

欢迎提交 Issue 和 Pull Request！如有问题或建议，请通过以下方式反馈：
- 提交 Issue
- 发起 Pull Request