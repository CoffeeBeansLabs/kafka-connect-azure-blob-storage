# azure-kafka-connector

$KAFKA represents the kafka directory

### Start Kafka
1. Run the zookeeper server

```
$KAFKA/bin/zookeeper-server-start.sh $KAFKA/config/zookeeper.properties
```

2. Run the kafka server

```
$KAFKA/bin/kafka-server-start.sh $KAFKA/config/server.properties
```

3. Create a topic

```
$KAFKA/bin/kafka-topics.sh --create --topic connect-demo --bootstrap-server localhost:9092 
```

4. Start the producer

```
$KAFKA/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic connect-demo
```


### Create jar of the connector
Inside the project directory run the following command:

```
mvn clean package
```

### Update plugin path in connect properties file
Open the properties file.

For distributed:

```
$KAFKA/config/connect-distributed.properties
```

For standalone:

```
$KAFKA/config/connect-standalone.properties
```

Add the target directory path of the connector project to the plugin.path property.

### Start the connector
1. Start connector

```
$KAFKA/bin/connect-distributed.sh $KAFKA/config/connect-distributed.properties
```

2. Configure the connector with REST API

```
curl -X POST -H "Content-Type: application/json" \ 
    -d @sink-connector-config-example.json \ 
    http://localhost:8083/connectors
```


