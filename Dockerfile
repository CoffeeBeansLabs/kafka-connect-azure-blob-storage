
# Extending confluent kafka-connect image
FROM confluentinc/cp-kafka-connect:7.2.0

# Copying schemas are only relevant if using <topic-name>.schema.url configuration
# Configuration is relevant only in case where incoming data is of type Json-string
# and format is either Parquet or Avro
#
# The schemas(file) can be passed to the configuration as below example:
#       <topic-name>.schema.url: file:///usr/share/java/<schema-file>
#
# Copying of schema is not needed if schema will be fetched from a remote service
#
# The schemas(remote service) can be passed to the configuration as below example:
#       <topic-name>.schema.url: https://link/to/schema-file

# COPY ./schemas /usr/share/java/

# Copying the connector and dependencies jar to the kafka directory
COPY ./target/kafka-connect-azure-blob-storage-1.0.0-SNAPSHOT-package /usr/share/java/kafka/

# `kafka-connect-azure-blob-storage-1.0.0-SNAPSHOT-package` does not contain
# kafka and confluent dependencies as it is provided by the confluent image