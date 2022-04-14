# Ref doc. https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html
import json
import os
import sys

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from random import randrange
from random_word import RandomWords
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import StringSerializer
from command_line_args_parser import parse_command_line_args


def produce_records(args):
    # Initialize schema registry client with schema-registry server url
    schema_registry_client = SchemaRegistryClient({"url": args.schema_registry})

    # Initialize JSON Serializer with JSON-schema and schema registry client
    json_serializer = JSONSerializer(
        load_json_schema_from_file(args.schema),
        schema_registry_client
    )

    string_serializer = StringSerializer()

    # Initialize kafka producer with Json serializer
    serializing_producer = SerializingProducer({"bootstrap.servers": args.bootstrap_servers,
                                                "value.serializer": string_serializer})

    # Get list of random words
    r = RandomWords()
    random_words = r.get_random_words(limit=500)

    # List of blobs
    blob_names = ['blob-1', 'blob-2', 'blob-3', 'blob-4', 'blob-5']

    start_timestamp = 1646109000000
    end_timestamp = 1646368200000

    for e in range(498):
        random_num = randrange(0, 2, 1)
        partition = randrange(0, 2, 1)
        word = random_words[e]
        timestamp = randrange(start_timestamp, end_timestamp)

        # data to be produced in kafka
        data = {
            'timestamp': timestamp,
            'name': blob_names[random_num],
            'num': e,
            'word': word
        }
        # produce to kafka
        serializing_producer.produce(topic=args.topic, key='nokey', value=json.dumps(data), partition=partition)
        print(json.dumps(data))

    serializing_producer.flush()


def load_json_schema_from_file(file_path):
    f = open(file_path)
    data = json.load(f)
    return json.dumps(data)


if __name__ == "__main__":
    produce_records(parse_command_line_args())
