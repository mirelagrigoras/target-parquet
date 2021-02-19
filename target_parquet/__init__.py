#!/usr/bin/env python3
import argparse
from datetime import datetime
from io import TextIOWrapper
import http.client
import simplejson as json
from jsonschema.validators import Draft4Validator
import os
import pandas as pd
import pkg_resources
import pyarrow as pa
import singer
import sys
import urllib
import psutil
import time
import threading
import gc
from multiprocessing import Process, Queue

from .helpers import flatten

_all__ = ["main"]

LOGGER = singer.get_logger()
LOGGER.setLevel(os.getenv("LOGGER_LEVEL", "INFO"))


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        LOGGER.debug("Emitting state {}".format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


class MemoryReporter(threading.Thread):
    """Logs memory usage every 30 seconds"""

    def __init__(self):
        self.process = psutil.Process()
        super().__init__(name="memory_reporter", daemon=True)

    def run(self):
        while True:
            LOGGER.debug(
                "Virtual memory usage: %.2f%% of total: %s",
                self.process.memory_percent(),
                self.process.memory_info(),
            )
            time.sleep(30.0)


def persist_messages(
    messages,
    destination_path,
    compression_method=None,
    streams_in_separate_folder=False,
):
    ## Static information shared among processes
    schemas = {}
    key_properties = {}
    validators = {}

    compression_extension = ""
    if compression_method:
        # The target is prepared to accept all the compression methods provided by the pandas module, with the mapping below,
        extension_mapping = {
            "SNAPPY": ".snappy",
            "GZIP": ".gz",
            "BROTLI": ".br",
            "ZSTD": ".zstd",
            "LZ4": ".lz4",
        }
        compression_extension = extension_mapping.get(compression_method.upper())
        if compression_extension is None:
            LOGGER.warning("unsuported compression method.")
            compression_extension = ""
            compression_method = None
    filename_separator = "-"
    if streams_in_separate_folder:
        LOGGER.info("writing streams in separate folders")
        filename_separator = os.path.sep
    if not os.path.exists(destination_path):
        os.makedirs(destination_path)
    ## End of Static information shared among processes

    # Object that signals shutdown
    _break_object = object()

    def producer(message_buffer: TextIOWrapper, w_queue: Queue):
        state = None
        try:
            for message in message_buffer:
                try:
                    message = singer.parse_message(message).asdict()
                except json.decoder.JSONDecodeError:
                    raise Exception("Unable to parse:\n{}".format(message))

                message_type = message["type"]
                if message_type == "RECORD":
                    if message["stream"] not in schemas:
                        raise Exception(
                            "A record for stream {} was encountered before a corresponding schema".format(
                                message["stream"]
                            )
                        )
                    stream_name = message["stream"]
                    validators[message["stream"]].validate(message["record"])
                    flattened_record = flatten(message["record"])
                    # Once the record is flattenned, it is added to the final record list, which will be stored in the parquet file.
                    w_queue.put((stream_name, flattened_record))
                    state = None
                elif message_type == "STATE":
                    LOGGER.debug("Setting state to {}".format(message["value"]))
                    state = message["value"]
                elif message_type == "SCHEMA":
                    stream = message["stream"]
                    schemas[stream] = message["schema"]
                    validators[stream] = Draft4Validator(message["schema"])
                    key_properties[stream] = message["key_properties"]
                else:
                    LOGGER.warning(
                        "Unknown message type {} in message {}".format(
                            message["type"], message
                        )
                    )
            w_queue.put((_break_object, None))
            return state
        except Exception as Err:
            w_queue.put((_break_object, None))
            raise Err

    def write_file(current_stream_name, record):
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S-%f")
        dataframe = pd.DataFrame(record)
        if streams_in_separate_folder and not os.path.exists(
            os.path.join(destination_path, current_stream_name)
        ):
            os.makedirs(os.path.join(destination_path, current_stream_name))
        filename = (
            current_stream_name
            + filename_separator
            + timestamp
            + compression_extension
            + ".parquet"
        )
        filepath = os.path.expanduser(os.path.join(destination_path, filename))
        dataframe.to_parquet(filepath, engine="pyarrow", compression=compression_method)
        ## explicit memory management. This can be usefull when working on very large data groups
        del dataframe
        return filepath

    def consumer(reciever):
        files_created = []
        current_stream_name = None
        # records is a list of dictionary of lists of dictionaries that will contain the records that are retrieved from the tap
        records = {}

        while True:
            (stream_name, record) = reciever.get()  # q.get()
            if type(stream_name) is object:
                files_created.append(
                    write_file(
                        current_stream_name,
                        records.pop(current_stream_name),
                    )
                )
                LOGGER.info(f"Wrote {len(files_created)} files")
                LOGGER.debug(f"Wrote {files_created} files")
                break
            if (stream_name != current_stream_name) and (current_stream_name != None):
                files_created.append(
                    write_file(
                        current_stream_name,
                        records.pop(current_stream_name),
                    )
                )
                ## explicit memory management. This can be usefull when working on very large data groups
                gc.collect()
            current_stream_name = stream_name
            if type(records.get(stream_name)) != list:
                records[stream_name] = [record]
            else:
                records[stream_name].append(record)

    q = Queue()
    t2 = Process(
        target=consumer,
        args=(q,),
    )
    t2.start()
    state = producer(messages, q)
    t2.join()
    return state


def send_usage_stats():
    try:
        version = pkg_resources.get_distribution("target-parquet").version
        conn = http.client.HTTPConnection("collector.singer.io", timeout=10)
        conn.connect()
        params = {
            "e": "se",
            "aid": "singer",
            "se_ca": "target-parquet",
            "se_ac": "open",
            "se_la": version,
        }
        conn.request("GET", "/i?" + urllib.parse.urlencode(params))
        conn.getresponse()
        conn.close()
    except:
        LOGGER.debug("Collection request failed")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Config file")

    args = parser.parse_args()
    if args.config:
        with open(args.config) as input_json:
            config = json.load(input_json)
    else:
        config = {}
        level = config.get("logging_level", None)
        if level:
            LOGGER.setLevel(level)
    if not config.get("disable_collection", False):
        LOGGER.info(
            "Sending version information to singer.io. "
            + "To disable sending anonymous usage data, set "
            + 'the config parameter "disable_collection" to true'
        )
        threading.Thread(target=send_usage_stats).start()
    # The target expects that the tap generates UTF-8 encoded text.
    input_messages = TextIOWrapper(sys.stdin.buffer, encoding="utf-8")
    if LOGGER.level == 0:
        MemoryReporter().start()
    state = persist_messages(
        input_messages,
        config.get("destination_path", "."),
        config.get("compression_method", None),
        config.get("streams_in_separate_folder", False),
    )

    emit_state(state)
    LOGGER.debug("Exiting normally")


if __name__ == "__main__":
    main()
