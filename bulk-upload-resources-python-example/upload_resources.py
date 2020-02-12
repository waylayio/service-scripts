import sys
import pandas
import numpy as np
from configparser import RawConfigParser
from datetime import datetime
import json
import requests

supported_timestamps = ['%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%dT%H:%M:%S.%fZ']

if len(sys.argv) < 2:
    print("Usage: `python ./main.py <csv-path> <configuration-path>`")
    exit()

csv_file_path = sys.argv[1]
config_path = sys.argv[2]

config = RawConfigParser()
config.read(config_path)


def get_auth(config: RawConfigParser):
    """ Get the authentication out of our config file """
    return config.get("domain", "client-key"), config.get("domain", "client-secret")


def convert_datetime_to_epoch(datetime_string: str):
    """ Converts the datetime to epoch in miliseconds there are two possible time formats"""
    try:
        return datetime.strptime(datetime_string, '%Y-%m-%dT%H:%M:%SZ').timestamp() * 1000
    except:
        return datetime.strptime(datetime_string, '%Y-%m-%dT%H:%M:%S.%fZ').timestamp() * 1000


def transform_data(data: pandas.DataFrame):
    """Transforms data from the csv format to the expected format bij the api"""
    data['timestamp'] = data['timestamp'].apply(
        lambda x: convert_datetime_to_epoch(x))
    data['metric'] = data['metric'].apply(lambda metric: metric.replace('waylay.resourcemessage.metric.', ''))
    return data


def chunck_to_message(chunck):
    """Makes entry in the dict with the key the value of the metric and with as value the value from de value key.
    Also drops the unneeded entries"""

    for row in chunck:
        row[row['metric']] = row.pop('value')
        row.pop('metric')
    return chunck


def send_messages(data_json_array: list, config: RawConfigParser):
    """Sends the json array to the waylay vendor in chunks of specific length (as defined in config)"""
    auth = get_auth(config)
    chunck_size = int(config.get('network', 'values-per-post'))
    sent = 0
    total = len(data_json_array)
    while len(data_json_array):
        chunck = data_json_array[:chunck_size]
        data_json_array = data_json_array[chunck_size:]
        messages = chunck_to_message(chunck)
        resp = requests.post("%s/messages?store=true&forward=false" % (config.get("domain", "data-path")),
                             json=messages, auth=auth)

        if not resp.ok:
            raise ConnectionError("Uploading failed.", resp.json())
        else:
            sent += chunck_size
            print('Sent ' + str(sent) + ' of ' + str(total))


def send_bulk_data(data: pandas.DataFrame, config: RawConfigParser):
    """Transforms data to json array and sends it to waylay service"""
    data_json_str = data.to_json(orient='records')
    data_json_array = json.loads(data_json_str)
    send_messages(data_json_array, config)


def run(csv_file_path: str, config: RawConfigParser):
    data = pandas.read_csv(csv_file_path, sep=",")
    data = transform_data(data)
    send_bulk_data(data, config)


run(csv_file_path, config)
