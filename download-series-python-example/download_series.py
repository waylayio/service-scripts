import sys
import pandas
import numpy as np
from configparser import RawConfigParser
import requests

if len(sys.argv) < 7:
    print("Usage: `python ./main.py <resource-name> +<metric-name> <from> <until> <output-file> <configuration-path>`")
    exit()

resource_name = sys.argv[1]
metric_names = sys.argv[2:-4]
from_epoch = int(sys.argv[-4])
until_epoch = int(sys.argv[-3])
output_file = sys.argv[-2]
config_path = sys.argv[-1]

config = RawConfigParser()
config.read(config_path)


def get_auth(config: RawConfigParser):
    """ Get the authentication out of our config file """
    return config.get("domain", "client-key"), config.get("domain", "client-secret")


def get_series(resource_name: str, metric_name: str, from_epoch: int, until_epoch: int, config: RawConfigParser):
    """ Do API call to get a series data """
    auth = get_auth(config)
    response = requests.get("%s/resources/%s/series/%s?from=%d&until=%d" % (
                            config.get("domain", "data-path"),
                            resource_name,
                            metric_name,
                            from_epoch,
                            until_epoch),
                          auth=auth)
    return pandas.DataFrame(np.array(response.json()['series']), columns=["timestamp", metric_name])


def run(resource_name: str, metric_names: str, from_epoch: int, until_epoch: int, output_file: str, config: RawConfigParser):
    """ Gather all series in one dataframe """
    complete_result = pandas.DataFrame(columns=['timestamp'])
    for metric_name in metric_names:
        complete_result = get_series(resource_name, metric_name, from_epoch, until_epoch, config)\
                            .merge(complete_result, 'outer', 'timestamp')

    complete_result.to_csv(output_file, index=False)


run(resource_name, metric_names, from_epoch, until_epoch, output_file, config_path)
