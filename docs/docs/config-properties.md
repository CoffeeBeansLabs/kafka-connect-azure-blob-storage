---
id: documentation
sidebar_position: 1
---

# Azure Blob Sink Connector Configuration properties

To use this connector, specify the name of the connector class in the `connector.class` configuration property.

```
connector.class: io.coffeebeans.connect.azure.blob.sink.AzureBlobSinkConnector
```

Connector-specific configuration properties are described below.

For overview of the connector refer to this [documentation](https://docs.confluent.io/kafka-connect-azure-blob-storage-sink/current/overview.html)


## Connector Parameters

`format`

The format class to use when writing data to the store.
* Type: String
* Valid values: `AVRO`, `JSON`, `PARQUET`, `BINARY`
* Importance: High



`flush.size`

Number of records written to store before invoking file commits.
* Type: int
* Importance: High


`rotate.interval.ms`

The time interval in milliseconds to invoke file commits. This configuration is useful when data ingestion rate is low and the connector didn’t write enough messages to commit files. The default value -1 means that this feature is disabled.
* Type: long
* Default: -1
* Importance: high


`schema.cache.config`

The size of the schema cache used in the Avro converter.
* Type: int
* Default: 1000
* Importance: low


`enhanced.avro.schema.support`

Enable enhanced Avro schema support in the Avro Converter. When set to true, this property preserves Avro schema package information and Enums when going from Avro schema to Connect schema. This information is added back in when going from Connect schema to Avro schema.
* Type: boolean
* Default: false
* Importance: low


`connect.meta.data`

Allow the Connect converter to add its metadata to the output schema.
* Type: boolean
* Default: true
* Importance: low

The connect.meta.data property preserves the following Connect schema metadata when going from Connect schema to Avro schema. The following metadata is added back in when going from Avro schema to Connect schema.

* doc
* version
* parameters
* default value
* name
* type

For detailed information and configuration examples for Avro converters listed above, see [Using Kafka Connect with Schema Registry](https://docs.confluent.io/platform/current/schema-registry/connect.html#avro).


`retry.backoff.ms`

The retry backoff in milliseconds. This config is used to notify Kafka connect to retry delivering a message batch or performing recovery in case of transient exceptions.
* Type: long
* Default: 5000
* Importance: low


`avro.codec`

The Avro compression codec to be used for output files. Available values: null, deflate, snappy and bzip2 (CodecSource is org.apache.avro.file.CodecFactory)

* Type: string
* Default: null
* Valid Values: [null, deflate, snappy, bzip2]
* Importance: low


`parquet.codec`

The Parquet compression codec to be used for output files.

* Type: string
* Default: snappy
* Valid Values: [none, snappy, gzip, brotli, lz4, lzo, zstd]
* Importance: low



## Azure Parameters

`azblob.connection.string`

The Azure account connection string;

* Type: string
* Importance: high


`azblob.container.name`

The container name. Must be between 3-63 alphanumeric and - characters

* Type: string
* Default: default
* Valid Values: [3,…,63]
* Importance: medium



`format.bytearray.extension`

Output file extension for ByteArrayFormat. Defaults to .bin

* Type: string
* Default: .bin
* Importance: low


`azblob.block.size`

The Block Size in Azure Multi-block Uploads.

* Type: int
* Default: 26214400
* Valid Values: [5242880,…,104857600]
* Importance: high


`azblob.retry.type`

The policy specifying the type of retry pattern to use. Should be either `EXPONENTIAL` or `FIXED`. An `EXPONENTIAL` policy will start with a delay of the value of `azblob.retry.backoff.ms` in (ms) then double for each retry up to a max in time (`azblob.retry.max.backoff.ms`) or total retries (`azblob.retry.retries`). A `FIXED` policy will always just use the value of `azblob.retry.backoff.ms` (ms) as the delay up to a total number of tries equal to the value of `azblob.retry.retries`.

* Type: string
* Default: EXPONENTIAL
* Valid Values: either one of [FIXED, EXPONENTIAL], or one of [exponential* fixed], or [null]
* Importance: low


`azblob.retry.retries`

Specifies the maximum number of retries attempts an operation will be tried before producing an error. A value of 1 means 1 try and 1 retries. The actual number of retry attempts is determined by the Azure client based on multiple factors, including, but not limited to - the value of this parameter, type of exception occurred, throttling settings of the underlying Azure client, etc.

* Type: int
* Default: 3
* Valid Values: [1,…]
* Importance: medium

`azblob.connection.timeout.ms`

Indicates the maximum time allowed for any single try of an HTTP request. NOTE: When transferring large amounts of data, the default value will probably not be sufficient. You should override this value based on the bandwidth available to the host machine and proximity to the Storage service. A good starting point may be something like (60 seconds per MB of anticipated-payload-size).

* Type: long
* Default: 30000
* Valid Values: [1,…,2147483647000]
* Importance: medium

`az.compression.type`

Compression type for file written to Azure. Applied when using JsonFormat or ByteArrayFormat. Available values: none and gzip.

* Type: string
* Default: none
* Valid Values: [none, gzip]
* Importance: low

`azblob.retry.backoff.ms`

Specifies the amount of delay to use before retrying an operation in milliseconds. The delay increases (exponentially or linearly) with each retry up to a maximum specified by MaxRetryDelay

* Type: long
* Default: 4000
* Valid Values: [0,…]
* Importance: low

`azblob.retry.max.backoff.ms`

Specifies the maximum delay in milliseconds allowed before retrying an operation.

* Type: long
* Default: 120000
* Valid Values: [1,…]
* Importance: low


`behavior.on.null.values`

How to handle records with a null value (for example, Kafka tombstone records). Valid options are ignore and fail.

* Type: string
* Default: fail
* Valid Values: [ignore, fail]
* Importance: low



## Storage Parameters

`topics.dir`

Top level directory to store the data ingested from Kafka.

* Type: string
* Default: topics
* Importance: high



`directory.delim`

Directory delimiter pattern

* Type: string
* Default: /
* Importance: medium



`file.delim`

File delimiter pattern

* Type: string
* Default: +
* Importance: medium


## Partitioner Parameters

`partition.strategy`

The partitioner to use when writing data to the store. You can use `DEFAULT`, which preserves the Kafka partitions; `FIELD`, which partitions the data to different directories according to the value of the partitioning field specified in partition.field.name; `TIME`, which partitions data according to ingestion time.

* Type: string
* Default: `DEFAULT`
* Importance: medium
* Dependents: `partition.field.name`, `partition.duration.ms`, `path.format`, `timezone`


`partition.field.name`

The name of the partitioning field when FieldPartitioner is used.

* Type: list
* Default: “”
* Importance: medium



`partition.duration.ms`

The duration of a partition milliseconds used by TimeBasedPartitioner. The default value -1 means that we are not using TimeBasedPartitioner.

* Type: long
* Default: -1
* Importance: medium



`path.format`

This configuration is used to set the format of the data directories when partitioning with `TIME`. The format set in this configuration converts the Unix timestamp to proper directories strings. For example, if you set `path.format='year'=YYYY/'month'=MM/'day'=dd/'hour'=HH`, the data directories will have the format /year=2015/month=12/day=07/hour=15/.

* Type: string
* Default: “”
* Importance: medium


`timezone`

The time zone to use when partitioning with `TIME`. Used to format and compute dates and times. All time zone IDs must be specified in the long format, such as America/Los_Angeles, America/New_York, and Europe/Paris, or UTC. 

* Type: string
* Default: `UTC`
* Importance: medium



`timestamp.extractor`

The extractor that gets the timestamp for records when partitioning with TimeBasedPartitioner. It can be set to Wallclock, Record or RecordField in order to use one of the built-in timestamp extractors or be given the fully-qualified class name of a user-defined class that extends the TimestampExtractor interface.

* Type: string
* Default: Wallclock
* Importance: medium



`timestamp.field`

The record field to be used as the timestamp by the timestamp extractor.

* Type: string
* Default: timestamp
* Importance: medium

