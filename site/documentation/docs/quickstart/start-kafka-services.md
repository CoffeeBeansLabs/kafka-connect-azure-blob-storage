---
sidebar_position: 3
---

# Start kafka services
Build the jar and start up all the kafka services.

## Build the jar

Create jar with following command

```bash
mvn clean package
```

> Jar will be created in the target folder

## Run docker containers

From parent directory run docker-compose.

```bash
docker-compose up -d
```

> zookeeperk, kafka broker, schema-registry and kafka-connect services will start

## Create kafka topic

1. Get terminal access of the kafka broker container with following command

    ```bash
    docker exec -it kafka bash
    ```

2. Create topic

    Run the following command

    ```bash
    kafka-topics.sh --create --topic connect-demo --bootstrap-server kafka:9092
    ```

    > Kafka topic with name 'connect-demo' will be created

## Configure kafka-connect

1. In a terminal run the following command

    ```bash
    curl -X POST -H "Content-Type: application/json" \
        -d @quickstart/config/config.json \
        http://localhost:8083/connectors
    ```

    > Connector will be configured

2. To check the installed connectors run the following command:

    ```bash
    curl http://localhost:8083/connectors
    ```