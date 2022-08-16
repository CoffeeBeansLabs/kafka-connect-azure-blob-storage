---
sidebar_position: 2
---

# Quickstart

## Pre-requisites

1. Docker installed
2. Azure storage explorer


### Installing Azure Storage Explorer
Install Azure storage explorer from this [link](https://azure.microsoft.com/en-in/features/storage-explorer/)

## Run Azurite

### What is azurite?
The Azurite open-source emulator provides a free local environment for testing your Azure blob, queue storage, and table storage applications. When you're satisfied with how your application is working locally, switch to using an Azure Storage account in the cloud. The emulator provides cross-platform support on Windows, Linux, and macOS.

### Starting blob storage service

#### Pull azurite docker image

Use the below docker command to pull the latest docker image:

```bash
docker pull mcr.microsoft.com/azure-storage/azurite
```

#### Start the blob storage service

Use the below command to run the azurite with blob storage service:

```bash
docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite azurite-blob --blobHost 0.0.0.0
```

> This will start blob storage service and listens on port `10000`

### Connect to storage explorer

1. Start azure storage explorer
2. Select resource: Local storage emulator
3. Remove queues port and tables port and leave everything as default
4. Click next and then connect

> Your containers will be visible under blob containers

## Start docker containers services
Build the jar and start up all the containers.

### Build the jar

Create jar with following command

```bash
mvn clean package
```

> Jar will be created in the target folder

### Run docker containers

From parent directory run docker-compose.

```bash
docker-compose up -d
```

> zookeeperk, kafka broker, schema-registry and kafka-connect and datagen services will start


### Download datagen connector

While containers are starting, lets download datagen connector. From parent directory on another terminal
run the below command:

```bash
sh quickstart/install-datagen.sh
```

This will download datagen source connector jar in datagen container inside confluent-hub-components folder.

### Configure datagen

Datagen is a source connector which generates mock data.
There are quickstart scripts for generating mock data in all formats.

Choose the script based on the which type of data you want to produce.

For eg. if you want to produce Avro serialized records, run below command:

```bash
curl -X POST -H "Content-Type: application/json" \
    -d @quickstart/config/datagen/avro-format.json \
    http://localhost:8084/connectors
```


### Configure kafka-connect

1. In a terminal run the following command

    ```bash
    curl -X POST -H "Content-Type: application/json" \
        -d @quickstart/config/connector/config.json \
        http://localhost:8083/connectors
    ```

    > Connector will be configured

2. To check the installed connectors run the following command:

    ```bash
    curl http://localhost:8083/connectors
    ```


### Delete connectors

1. Run below command to stop datagen connector

    ```bash
        curl -X DELETE http://localhost:8084/connectors/format-avro
    ```

 1. Run below command to stop Azure blob sink connector

    ```bash
        curl -X DELETE http://localhost:8083/connectors/quickstart
    ```


## Stop containers

Run the following command

```bash
docker-compose stop
```

> This will stop the containers

or

```bash
docker-compose down
```

> This will stop and remove all the containers