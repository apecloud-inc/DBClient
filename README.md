# DBClient
Using for an outside user to access databases in KubeBlocks with client SDK under different network environments.
We also provide a unified database command line tool named "oneclient" to support basic and advanced test interfaces, like max connections stress, benchmark and other query tests.

## Supported Databases & Networks
KubeBlocks supports a wide range of databases, nearly 30+ types. 
Here we only show examples for some of them, especially the frequently used ones.
If your applications are running under same k8s with KubeBlocks, just access the databases with ClusterIP, Pod headless service, and they are easy to take on.
When your applications access the databases from another k8s or baremetal environments, NodePort and LoadBalancer are always recommended network solutions.
So here we give some examples for NodePort and LoadBalancer.

## How to run advanced database tests
### Build OneClient

```bash
gradle build

You will get oneclient jar at:

./build/libs/oneclient-1.0-all.jar
```

### Usage of OneClient


### Adanced Test Cases
#### Run any query

Access the user table:
bash-3.2$ java -jar ./build/libs/oneclient-1.0-all.jar --host=127.0.0.1 --user=postgres --database=postgres --password=g67rtlm8 --test=query --query="select * from pg_user limit 1" --port=5432 --dbtype=postgresql
```bash
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
Query: select * from pg_user limit 1
```

#### Run Connection Stress test

gradle runOneclient --args="--password='g67rtlm8' --port=5432  --database=postgres --user=postgres --dbtype=postgresql --test=connectionstress --connections=10"

or:

java -jar ./build/libs/oneclient-1.0-all.jar --host=127.0.0.1 --user=postgres --database=postgres --password='g67rtlm8' --test=connectionstress --connections=10 --port=5432 --dbtype=postgresql
```bash
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

#### Run a benchmark

 java -jar ./build/libs/oneclient-1.0-all.jar --host=127.0.0.1 --user=postgres --database=postgres --password=g67rtlm8 --test=benchmark --query="select * from pg_user limit 1" --port=5432 --dbtype=postgresql
 ```bash
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
Query: select * from pg_user limit 1
 ```

#### Run on Pod

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
        - "g67rtlm8"
        - "--test"
        - "query"
        - "--query"
        - "select * from pg_user limit 1"
        - "--dbtype"
        - "postgresql"
  restartPolicy: Never
```