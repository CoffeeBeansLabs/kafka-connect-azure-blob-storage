#Quickstart

###Start Azurite
 
Download Azurite docker image

```
docker pull mcr.microsoft.com/azure-storage/azurite
```

Run the azurite blob storage service

```
docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite azurite-blob --blobHost 0.0.0.0
```

This will start blob storage service and listens on port 10000

### Start Azure Storage Explorer

Download Microsoft Azure Storage Explorer

https://azure.microsoft.com/en-in/features/storage-explorer/

After installing run the applicartion.

#### Connect to azurite

1. Select resource: Local storage emulator
2. Remove queues port and tables port and leave everything as default
3. Click next
4. Click connect

Your containers will be visible under Blob Containers

### Start Kafka Connect

#### Create jar

Run the following command:

```
mvn clean package
```

From the project directory run docker-compose:

```
docker-compose up -d
```

Zookeeper, kafka broker and kafka-connect will start

#### Create the kafka topic

In another terminal execute

```
docker exec -it kafka bash
```

Inside the container execute following command:

```
kafka-topics.sh --create --topic connect-demo --bootstrap-server kafka:9092
```
Kafka topic with name connect-demo will be created.

#### Create the console producer

From the inside the kafka broker container run the following command:

```
kafka-console-producer.sh --topic connect-demo --bootstrap-server kafka:9092
```

### Configuring kafka connector

In another terminal run the following command:

```
curl -X POST -H "Content-Type: application/json" \
    -d @example/config/sink-connector-config-example.json \
    http://localhost:8083/connectors
```

This will configure the connector.

To check the installed connectors run the following command:

```
curl http://localhost:8083/connectors
```

### Working
Create a blob container with name provided in the config json file.
Paste a json on the producer console and hit enter.
The json should be stored in the blob container.

### Stop the containers

Run the following command:

To stop containers:
```
docker-compose stop
```

or to stop and remove containers:
```
docker-compose down
```