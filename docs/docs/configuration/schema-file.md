---
id: schema-file
title: Schema File
sidebar_position: 2
---

# Schema File Configuration

This doc will guide on using the schema url configuration for types that
do not have their own schema.

<hr />

## Overview

Writing data to sink in `Avro` or `Parquet` format requires schema.

If incoming data is of following type then there is no need of 
specifying the schema explicitly as it already gets the schema from schema registry:

 - Avro
 - Protobuf
 - Json with Schema Registry
 - Json with embedded schema

But for below formats there is not info about the schema to the connector:

 - Json without schema
 - Json-String

To be able to write data in either Avro or Parquet format we need to explicitly
specify the schema to the connector.

There are two ways you can specify the schema:

 1. Pass path to the Avro schema file
 2. Pass the URL to the Avro schema file


## Writing schema file

There are few points to be implemented before leveraging this featue:

 1. For field `type`, `null` should precede any other data type.

 For eg.
 ```
 {"name": "uid",  "type": ["null", "string"], "default": null}
 ``` 

 <br />

 2. The `default` should always be `null` for all optional fields

  For eg.
  ```
  {"name": "isSatisfied", "type": ["null", "boolean"], "default": null},
  ```

Here is a sample Avro schema file:

```json
{"namespace": "topics.avro",
    "type": "record",
    "name": "sample_schema",
    "fields": [
      {"name": "timestamp", "type": ["null", "long"], "default": null},
      {"name": "uid",  "type": ["null", "string"], "default": null},
      {"name": "isConditionSatisfied", "type": ["null", "boolean"], "default": null},
      {"name": "partition", "type": ["null", "int"], "default": null},
      {"name": "locationDetails", "type": {"name": "locationDetailsObj", "type": "record", "fields": [
          {"name": "city", "type": ["null", "string"], "default": null},
          {"name": "state", "type": ["null", "string"], "default": null},
          {"name": "latitude", "type": ["null", "double"], "default": null},
          {"name": "longitude", "type": ["null", "double"], "default": null},
          {"name": "country", "type": ["null", "string"], "default": null},
          {"name": "timezone", "type": ["null", "string"], "default": null}
      ]}},
      {"name": "analytics", "type": {"name": "analyticsObj", "type": "record", "fields": [
          {"name": "analyticsChildObj", "type": ["null", "string"], "default": null}
      ]}}
    ]
}
```

## Passing schema file

If deploying using Docker image, you can copy the schema file in the image while building it.

For detailed instructions refer to this [doc](https://coffeebeanslabs.github.io/kafka-connect-azure-blob-storage/docs/deployment/kubernetes#copying-schema-files)


## Passing URL to schema

If your schema is hosted at a remote service, you can pass the 
URL of that service.

Eg.
```yaml
topic-name.schema.url: https://some.host.com/path/to/schema
```