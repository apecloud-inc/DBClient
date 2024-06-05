# DBClient
Using for an outside user to access databases in KubeBlocks with client SDK under different network environments.

## Supported Databases & Networks
KubeBlocks supports a wide range of databases, nearly 30+ types. 
Here we only show examples for some of them, especially the frequently used ones.
If your applications are running under same k8s with KubeBlocks, just access the databases with ClusterIP, Pod headless service, and they are easy to take on.
When your applications access the databases from another k8s or baremetal environments, NodePort and LoadBalancer are always recommended network solutions.
So here we give some examples for NodePort and LoadBalancer.

## How to run examples
### MySQL
gradle runMySQL

### PostgreSQL
gradle runPostgreSQL

### MongoDB Replicaset
gradle runMongoDB

### Sentinel Redis
gradle runSentinelRedis
