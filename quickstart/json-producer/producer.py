# Ref doc. https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html

from random import randrange
from random_word import RandomWords
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from command_line_args_parser import parse_command_line_args


def produce_records(args):

    schema_registry_client = SchemaRegistryClient({"url": args.schema_registry})
    json_serializer = JSONSerializer(json_schema(), schema_registry_client)

    serializing_producer = SerializingProducer({"bootstrap.servers": args.bootstrap_servers, "value.serializer": json_serializer})

    r = RandomWords()
    blob_names = ['blob-1', 'blob-2', 'blob-3', 'blob-4', 'blob-5']
    random_words = r.get_random_words(limit=500)

    for e in range(499):
        data = {
            'name': blob_names[randrange(0, 3, 1)],
            'num': e,
            'word': random_words[e]
        }
        serializing_producer.produce(topic=args.topic, key='nokey', value=data)
        print(data)

    serializing_producer.flush()


def json_schema():
    value_schema = """
    {
      "type": "object",
      "title": "test",
      "properties": {
        "name": {
          "type": "string"
        },
        "num": {
          "type": "integer"
        },
        "word": {
          "type": "string"
        }
      },
      "required": [
        "name",
        "num",
        "word"
      ]
    }
    """
    return value_schema


if __name__ == "__main__":
    produce_records(parse_command_line_args())
