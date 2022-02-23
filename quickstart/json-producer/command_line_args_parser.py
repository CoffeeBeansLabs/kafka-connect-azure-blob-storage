from argparse import ArgumentParser


def parse_command_line_args():
    arg_parser = ArgumentParser()

    arg_parser.add_argument("--topic", required=True, help="Topic name")
    arg_parser.add_argument("--bootstrap-servers", required=False, default="localhost:9092", help="bootstrap servers")
    arg_parser.add_argument("--schema-registry", required=False, default="http://localhost:8081",
                            help="schema registry server")

    return arg_parser.parse_args()
