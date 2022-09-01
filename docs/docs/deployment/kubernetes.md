---
id: kubernetes
title: Kubernetes
sidebar_position: 1
---

# Deployment With Kubernetes

Confluent provides docker [images](https://github.com/confluentinc/kafka-images) which we can use to deploy our connector
without much effort.

The source code includes a [Dockerfile](https://github.com/CoffeeBeansLabs/kafka-connect-azure-blob-storage/blob/main/Dockerfile) which extends the Confluent provided cp-kafka-connect
docker image and copies the connector jar and the dependency jars to the kafka-connect plugin path.

:::info

`kafka-connect-azure-blob-storage-${CONNECTOR_VERSION}-package` does not include 
kafka and confluent dependencies as it is provided by the confluent image

:::

<br />
<hr />
<br />

To use the provided Dockerfile and confluent provided docker images follow the steps mentioned below:

## Building project

To create the connector and dependency jars run the following command from the project root directory:

```bash
./mvnw clean install
```

This generates two directories with jars in the target directory:

 - `kafka-connect-azure-blob-storage-${CONNECTOR_VERSION}-development`
 - `kafka-connect-azure-blob-storage-${CONNECTOR_VERSION}-package`

## Building docker image

### Copying schema files

Copying schemas are only relevant if using `topic-name.schema.url` configuration.
Configuration is relevant only in case when incoming data is of type Json-string
and format is either Parquet or Avro.

The schemas (file) can be passed to the configuration as below example:

```yaml
topic-name.schema.url: file:///usr/share/java/schema-file
```

Copying of schema is not needed if schema will be fetched from a remote service

The schemas (remote service) can be passed to the configuration as below example:

```yaml
topic-name.schema.url: https://link/to/schema-file
```

Make changes to the Dockerfile according to your use-case.
If changing the destination directory, make sure it has required access permissions

<br />

### Building Image

To build the docker image from the provided Dockerfile, run this command from the project root directory :

```zsh
docker build . -t {Your docker image name}:{Version} --build-arg CONNECTOR_VERSION={Connector version}
```

Replace
 - `{Your docker image name}` with your docker image name
 - `{Version}` with version of the docker image
 - `{Connector version}` with your connector jar version

<br />

### Pushing to registry

To push this docker image to the registry use this command :

```zsh
docker push {Your docker image tag}
```


## Writing kubernetes deployment file

Below is a <b>sample</b> kubernetes deployment configuration. Actual configuration may differ based
on your requirements.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-connect-azure-blob-storage
  namespace: development
spec:
  replicas: 2
  selector:
    matchLabels:
      app: kafka-connect-azure-blob-storage
  template:
    metadata:
      labels:
        app: kafka-connect-azure-blob-storage
    spec:
      containers:
      - env:
        - name: CONNECT_REST_ADVERTISED_HOST_NAME
          valueFrom:
            fieldRef:
             fieldPath: status.podIP
        - name: CONNECT_BOOTSTRAP_SERVERS
          value: "kafka.broker.address"
        - name: CONNECT_GROUP_ID
          value: "kcabs-cluster"
        - name: CONNECT_CONFIG_STORAGE_TOPIC
          value: "_kcabs-connect-configs"
        - name: CONNECT_OFFSET_STORAGE_TOPIC
          value: "_kcabs-connect-offset"
        - name: CONNECT_STATUS_STORAGE_TOPIC
          value: "_kcabs-connect-status"
        - name: CONNECT_KEY_CONVERTER
          value: "org.apache.kafka.connect.storage.StringConverter"
        - name: CONNECT_VALUE_CONVERTER
          value: "org.apache.kafka.connect.storage.StringConverter"
        image: docker-image
        imagePullPolicy: IfNotPresent
        name: kafka-connect-azure-blob-storage
        ports:
          - containerPort: 8083
        resources:
          limits:
            memory: 4Gi
          requests:
            memory: 2Gi
---
apiVersion: v1
kind: Service
metadata:
  name: kafka-connect-azure-blob-storage
  namespace: development
spec:
  selector:
    app: kafka-connect-azure-blob-storage
  ports:
    - name: http
      port: 8083
  type: ClusterIP
```

Replace:

 - `CONNECT_BOOTSTRAP_SERVERS` value to kafka broker address
 - `image` value to your docker image address


<br />

The below configuration sets the pod IP address as environment variable with key `CONNECT_REST_ADVERTISED_HOST_NAME`

```yaml
env:
  - name: CONNECT_REST_ADVERTISED_HOST_NAME
    valueFrom:
      fieldRef:
        fieldPath: status.podIP
```

All pods have unique `CONNECT_REST_ADVERTISED_HOST_NAME` which they broadcast to the kafka-connect cluster 
which they are part of. It is used for communication between worker nodes.


## Deploying

### Creating deployment

To perform the deploy run the below command :

```bash
kubectl apply -f configuration.yaml
```

### Deleting deployment

To delete the deployment and remove pods run the below command:

```bash
kubectl delete -f configuration.yaml
```