# Build DBClient Image

docker buildx build -f docker/Dockerfile -t docker.io/apecloud/dbclient:latest --platform=linux/amd64,linux/arm64 . --push

## Run Connection on Pod
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