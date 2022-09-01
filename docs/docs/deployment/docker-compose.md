---
id: docker-compose
title: Docker Compose
sidebar_position: 3
---

# Deployment With Docker Compose

The source code contains a Docker compose file which can be used to 
run the connector in development mode.

:::note

Running multiple connectors in distributed mode in Docker compose is <b>not</b> supported by confluent images.

:::

## Building the project

To build the project run the following command:

```bash
./mvnw clean install
```


## Starting Kafka services

To start Zookeeper and other kafka services run the following command:

```bash
docker compose up -d
```

This will start following services:

 - Zookeeper
 - Kafka broker
 - Schema registry
 - Kafka-Connect (Connector jar is mounted)
 - Kafka-Connect (To be used for Datagen source connector for quickstart)