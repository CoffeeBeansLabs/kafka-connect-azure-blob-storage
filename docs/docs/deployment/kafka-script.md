---
id: kafka-console-script
title: Kafka Console Script
sidebar_position: 2
---

# Deployment With Kafka Console Script

Kafka connector jars can be run using Kafka console scripts.

Follow the steps to run the connector using console scripts.

## Downloading console scripts

Download the binary files from [Apache Kafka downloads](https://kafka.apache.org/downloads). For this doc
we will be downloading `kafka_2.12-3.2.1.tgz`

Unzip it.

For the following steps we will be using `$KAFKA` to indicate path to the `kafka_2.12-3.2.1` directory.

## Start Zookeeper

To start the zookeeper run following command:

```bash
$KAFKA/bin/zookeeper-server-start.sh $KAFKA/config/zookeeper.properties
```

This starts zookeeper.


## Start Kafka broker

To start the kafka broker run following command:

```bash
$KAFKA/bin/kafka-server-start.sh $KAFKA/config/server.properties
```

This starts kafka broker. Default port is `9092`.



## Update Kafka connect configuration

 - Open the file `$KAFKA/config/connect-distributed.properties`.

 - Update the `bootstrap.servers` to `localhost:9092` (default Kafka broker address).

 - Add the path of your project `target` folder in the `plugin.path` configuration.

Make other configuration changes as per your requirements.


## Building connector jar

To build the connector jar run following command from the project root directory:

```bash
./mvnw clean install -Pstandalone
```

This creates a uber jar which contains all the dependency jars.


## Starting Kafka Connect

To start Kafka connect run following command:

```bash
$KAFKA/bin/connect-distributed.sh $KAFKA/config/connect-distributed.properties
```