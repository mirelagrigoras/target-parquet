#!/usr/bin/env python3
import argparse
import collections
import csv
from datetime import datetime
import io
import http.client
import json
from jsonschema.validators import Draft4Validator
import os
import pandas as pd
import pkg_resources
import pyarrow
import singer
import sys
import urllib
import threading

LOGGER = singer.get_logger()

def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        LOGGER.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()

def flatten(dictionary, parent_key = '', sep = '__'):
    '''Function that flattens a nested structure, using the separater given as parameter, or uses '__' as default
        E.g:
         dictionary =  {
                            'key_1': 1,
                            'key_2': {
                                   'key_3': 2,
                                   'key_4': {
                                          'key_5': 3,
                                          'key_6' : ['10', '11']
                                     }     
                            }
                           }
        By calling the function with the dictionary above as parameter, you will get the following strucure:  
            {
                 'key_1': 1,
                 'key_2__key_3': 2,
                 'key_2__key_4__key_5': 3,
                 'key_2__key_4__key_6': "['10', '11']"
             }
    '''
    items = []
    for k, v in dictionary.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep = sep).items())
        else:
            items.append((new_key, str(v) if type(v) is list else v))
    return dict(items)

def set_mixed_type_column_types(df):
    for col in df.columns:
        col_has_mixed_types = (df[[col]].applymap(type) != df[[col]].iloc[0].apply(type)).any(axis=1)
        if len(df[col_has_mixed_types]) > 0:
            df[col] = df[col].astype(str)

        if df[col].dtype == list:
            df[col] = df[col].astype(str)
    return df
        
def persist_messages(messages, destination_path, compression_method = None):
    state = None
    schemas = {}
    key_properties = {}
    headers = {}
    validators = {}
    records = []  #  A list of dictionaries that will contain the records that are retrieved from the tap

    for message in messages:
        try:
            message = singer.parse_message(message).asdict()
        except json.decoder.JSONDecodeError:
            raise Exception("Unable to parse:\n{}".format(message))
        
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        
        message_type = message['type']
        if message_type == 'STATE':
            LOGGER.debug('Setting state to {}'.format(message['value']))
            state = message['value']
        elif message_type == 'SCHEMA':
            stream = message['stream']
            schemas[stream] = message['schema']
            validators[stream] = Draft4Validator(message['schema'])
            key_properties[stream] = message['key_properties']
        elif message_type == 'RECORD':
            if message['stream'] not in schemas:
                raise Exception("A record for stream {} was encountered before a corresponding schema".format(message['stream']))
            stream_name = message['stream']
            validators[message['stream']].validate(message['record'])
            flattened_record = flatten(message['record'])
            # Once the record is flattenned, it is added to the final record list, which will be stored in the parquet file.
            records.append(flattened_record)
            state = None
        else:
            LOGGER.warning("Unknown message type {} in message {}".format(message['type'], message))
    if len(records) == 0:
        # If there are not any records retrieved, it is not necessary to create a file.
        LOGGER.info("There were not any records retrieved.")
        return state
    # Create a dataframe out of the record list and store it into a parquet file with the timestamp in the name.
    dataframe = set_mixed_type_column_types(pd.DataFrame(records))
    filename =  stream_name + '-' + timestamp + '.parquet'
    filepath = os.path.expanduser(os.path.join(destination_path, filename))
    if compression_method:
        # The target is prepared to accept all the compression methods provided by the pandas module, with the mapping below, 
        # but, at the moment, pyarrow only allow gzip compression. 
        extension_mapping = {
            'gzip' : '.gz',
            'bz2'  : '.bz2', 
            'zip'  : '.zip',
            'xz'   : '.xz'
        }
        dataframe.to_parquet(filepath + extension_mapping[compression_method], engine = 'pyarrow', compression = compression_method)
    else:
        dataframe.to_parquet(filepath, engine = 'pyarrow')
    return state

def send_usage_stats():
    try:
        version = pkg_resources.get_distribution('target-parquet').version
        conn = http.client.HTTPConnection('collector.singer.io', timeout = 10)
        conn.connect()
        params = {
            'e': 'se',
            'aid': 'singer',
            'se_ca': 'target-parquet',
            'se_ac': 'open',
            'se_la': version,
        }
        conn.request('GET', '/i?' + urllib.parse.urlencode(params))
        response = conn.getresponse()
        conn.close()
    except:
        LOGGER.debug('Collection request failed')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help = 'Config file')
    
    args = parser.parse_args()
    if args.config:
        with open(args.config) as input_json:
            config = json.load(input_json)
    else:
        config = {}
    if not config.get('disable_collection', False):
        LOGGER.info('Sending version information to singer.io. ' +
                    'To disable sending anonymous usage data, set ' +
                    'the config parameter "disable_collection" to true')
        threading.Thread(target = send_usage_stats).start()
    # The target expects that the tap generates UTF-8 encoded text.
    input_messages = io.TextIOWrapper(sys.stdin.buffer, encoding = 'utf-8')
    state = persist_messages(input_messages,
                             config.get('destination_path', ''),
                             config.get('compression_method'))

    emit_state(state)
    LOGGER.debug("Exiting normally")

if __name__ == '__main__':
    main()