# QUICKSTART

Quickly setup local azure blob storage emulator and kafka services to test the connector.

<br>

## Pre-requisites

1. Docker installed
2. Python installed
3. Azure storage explorer

<br>

Install Azure storage explorer from below link:

https://azure.microsoft.com/en-in/features/storage-explorer/

<br><hr><br>

## Starting blob storage service

<br>

1. Start azurite (local azure blob storage emulator) with the following docker command

```bash
docker pull mcr.microsoft.com/azure-storage/azurite
```

<br>

2. Run azurite blob storage service

```bash
docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite azurite-blob --blobHost 0.0.0.0
```

> This will start blob storage service and listens on port 10000

<br>

3. Connect azurite with azure storage explorer

    * Start azure storage explorer
    * Select resource: Local storage emulator
    * Remove queues port and tables port and leave everything as default
    * Click next and then connect

> Your containers will be visible under blob containers

<br><hr><br>

## Starting kafka services

<br>

1. Create jar with following command

```bash
mvn clean package
```

> Jar will be created in the target folder

<br>

2. Start docker containers. From parent directory run docker-compose.

```bash
docker-compose up -d
```

<br>

> zookeeperk, kafka broker, schema-registry and kafka-connect services will start

<br><hr><br>

## Create kafka topic

<br>

1. Get terminal access of the kafka broker container with following command

```bash
docker exec -it kafka bash
```

<br>

2. Create topic. Run the following command

```bash
kafka-topics.sh --create --topic connect-demo --bootstrap-server kafka:9092
```

> Kafka topic with name 'connect-demo' will be created

<br><hr><br>

## Configure kafka-connect

<br>

1. In a terminal run the following command

```bash
curl -X POST -H "Content-Type: application/json" \
    -d @quickstart/config/config.json \
    http://localhost:8083/connectors
```

> Connector will be configured

<br>

2. To check the installed connectors run the following command:

```bash
curl http://localhost:8083/connectors
```

<br><hr><br>

## Producing records

1. Run the following command (for json with schema producer)

```bash
python3 quickstart/producers/json-producer/producer.py --topic connect-demo --bootstrap-servers localhost:9093
```

> topic is mandatory

> Default value of bootstrap-servers is ```localhost:9092```

> Default value of schema-registry is ```http://localhost:8081```

> Default value of schema is ```../../schemas/json-schema.json```

<br><hr><br>

## Stopping all the services

<br>

1. Stop all kafka services. Run the following command

```bash
docker-compose stop
```

> This will stop the containers

or

```bash
docker-compose down
```

> This will stop and remove all the containers
