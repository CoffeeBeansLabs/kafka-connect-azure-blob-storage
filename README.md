# Kafka Azure Blob Sink Connector

- [About](#about)
- [Installing](#installing)
- [Configurations](#configurations)
  - [Connection](#configurations-connection)
  - [Partition](#configurations-partition)
    - [Default partitioning](#configurations-partition-default)
    - [Field partitioning](#configurations-partition-field)
    - [Time partitioning](#configurations-partition-time)
  - [Rolling file](#configurations-rolling-file)

# About
Kafka Azure Blob Sink Connector will consume records from kafka topic(s) and will
store it to the azure blob storage.

# Installing
For quickstart please refer to this <a href="https://github.com/CoffeeBeansLabs/azure-kafka-connector/blob/main/quickstart/QUICKSTART.md" target="_blank">documentation</a>.

# Configurations

## Connection
Azure blob storage connection related configurations.

### Connection url
key: ```connection.url```<br>
value: azure blob storage connection string

### Container name
key: ```container.name```<br>
value: name of the container<br>
default: ```default```

## Partition
Configurations to perform partition operations on the incoming records

### Parent directory
key: ```topic.dir```<br>
value: name of the parent directory

### Partition strategy
key: ```partition.strategy```<br>
valid values: ```TIME,FIELD```<br>
default: ```DEFAULT```

### Default partitioning

### Field partitioning

### Time partitioning

## Rolling file

