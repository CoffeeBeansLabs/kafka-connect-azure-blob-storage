from argparse import ArgumentParser


def parse_command_line_args():
    arg_parser = ArgumentParser()

    # Kafka topic name to produce messages
    arg_parser.add_argument("--topic", required=True, help="Kafka topic name")

    # bootstrap server address
    arg_parser.add_argument("--bootstrap-servers", required=False, default="localhost:9092",
                            help="bootstrap servers, default is localhost:9092")

    # schema registry server address
    arg_parser.add_argument("--schema-registry", required=False, default="http://localhost:8081",
                            help="schema registry server, default is http://localhost:8081")

    # path for schema file
    arg_parser.add_argument("--schema", required=False, default="../../schemas/json-schema.json",
                            help="path for schema file, if not set default schema will be used")

    return arg_parser.parse_args()
