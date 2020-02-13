import sys
from configparser import RawConfigParser

import numpy as np
import pandas
import requests
from requests import HTTPError

if len(sys.argv) < 3:
    print("Usage: `python ./main.py <csv-path> <resource_id> <configuration-path>`")
    exit()

input_path = sys.argv[1]
resource_name = sys.argv[2]
config_path = sys.argv[3]

config = RawConfigParser()
config.read(config_path)


def get_auth(config: RawConfigParser):
    """ Get the authentication out of our config file """
    return config.get("domain", "client-key"), config.get("domain", "client-secret")


def send_messages(data: pandas.DataFrame, config: RawConfigParser, chunk_size: int = 1000):
    """ Bulk send messages to the broker """

    auth = get_auth(config)
    while len(data):
        chunk = data[:chunk_size]
        data = data[chunk_size:]
        messages = chunk.to_dict("records")

        resp = requests.post("%s/messages?store=true&forward=false" % (config.get("domain", "data-path")), json=messages, auth=auth)
        _validate_response(resp, "Uploading messages failed.")


def get_metric_definition(name: str, data: pandas.Series):
    """ Create the definition of a metric """

    return {
        "name": name,
        "valueType": "double" if np.issubdtype(data.dtype, np.number) else "string",
        "metricType": "gauge",
    }


def create_resource(resource_name: str, data: pandas.DataFrame, config: RawConfigParser):
    """ Create a resource """

    metrics = [get_metric_definition(col, data[col]) for col in data if col != "timestamp"]

    auth = get_auth(config)
    resp = requests.post(config.get("domain", "resource-path") + "/api/resources/", auth=auth, json={
        "id": resource_name,
        "name": resource_name,
        "metrics": metrics,
    })
    _validate_response(resp, "Creating resource failed.")

def _validate_response(resp, error_text):
    try:
        resp.raise_for_status()
    except HTTPError as http_error:
        try:
            json_data = resp.json()
            raise ConnectionError(error_text, json_data)
        except ValueError:
            raise http_error

def run(input_path: str, resource_name: str, config: RawConfigParser):
    """ Create a resource and pre-process the messages """

    data = pandas.read_csv(input_path, sep=";")

    create_resource(resource_name, data, config)

    data["resource"] = resource_name
    send_messages(data, config)


run(input_path, resource_name, config)
