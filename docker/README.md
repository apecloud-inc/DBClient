# DBClient Docker Image

This directory contains the Docker configuration for building and running the DBClient (OneClient) container image.

## Building the Image

### Multi-architecture Build (AMD64 and ARM64)

```bash
docker buildx build -f docker/Dockerfile \
  -t docker.io/apecloud/dbclient:latest \
  --platform=linux/amd64,linux/arm64 \
  . --push
```

### Single Architecture Build

For local testing on AMD64:
```bash
docker build -f docker/Dockerfile -t apecloud/dbclient:latest .
```

For local testing on ARM64 (e.g., Apple Silicon):
```bash
docker build --platform linux/arm64 -f docker/Dockerfile -t apecloud/dbclient:latest .
```

## Running the Container

### Basic Usage

Run a simple query:
```bash
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

### Connection Stress Test

```bash
docker run --rm apecloud/dbclient:latest \
  --host=127.0.0.1 \
  --user=postgres \
  --database=postgres \
  --password=mysecretpassword \
  --test=connectionstress \
  --connections=10 \
  --port=5432 \
  --dbtype=postgresql
```

### Benchmark Test

```bash
docker run --rm apecloud/dbclient:latest \
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

## Kubernetes Usage

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

View the logs:
```bash
kubectl logs <pod-name>
```

## Supported Database Types

The DBClient image supports 30+ database types including:

- **Relational**: MySQL, PostgreSQL, Oracle, SQL Server, MariaDB, TiDB, OceanBase, GaussDB, etc.
- **NoSQL**: MongoDB, Redis, Cassandra
- **Search**: Elasticsearch, OpenSearch
- **Time-Series**: InfluxDB, TDengine, VictoriaMetrics, VictoriaLogs
- **Message Queues**: Kafka, RabbitMQ, RocketMQ, Pulsar
- **Vector**: Milvus, Qdrant, Nebula
- **Others**: ClickHouse, Doris, StarRocks, ZooKeeper, Etcd, MinIO, etc.

See the main [README.md](../README.md) for complete usage documentation and examples.

## Network Configuration

### Accessing Databases in Kubernetes

When running in Kubernetes, you can access databases using:

1. **ClusterIP** (same cluster):
   ```bash
   --host=<service-name>.<namespace>.svc.cluster.local
   ```

2. **NodePort** (external access):
   ```bash
   --host=<node-ip> --port=<node-port>
   ```

3. **LoadBalancer** (external access):
   ```bash
   --host=<load-balancer-ip> --port=<lb-port>
   ```

### Example: Connect to Database in Another Namespace

```bash
docker run --rm apecloud/dbclient:latest \
  --host=mydb.database.svc.cluster.local \
  --user=admin \
  --password=secret \
  --database=mydb \
  --port=5432 \
  --dbtype=postgresql \
  --test=query \
  --query="SELECT version()"
```

## Troubleshooting

### Container Exits Immediately

The container is designed to run a single command and exit. Use `docker run --rm` to automatically clean up the container after execution.

### Network Connectivity Issues

Ensure the container can reach your database:
- For local databases, use `--network host` flag
- For Kubernetes services, ensure proper DNS resolution
- Check firewall rules and security groups

```bash
# Example with host network
docker run --rm --network host apecloud/dbclient:latest \
  --host=127.0.0.1 \
  --user=postgres \
  --password=secret \
  --database=test \
  --port=5432 \
  --dbtype=postgresql
```

## Image Tags

- `latest`: Most recent build
- Version tags (e.g., `v1.0`, `v1.1.0`): Specific releases

## Registry

The image is hosted on Docker Hub:
- Repository: [docker.io/apecloud/dbclient](https://hub.docker.com/r/apecloud/dbclient)
