from __future__ import division
import math
import datetime
import time
import configparser
import sys
import csv
import unicodedata
import os
import boto3
import json



def csv_writer(data, path):
    """
    Write data to a CSV file path
    """
    with open(path, "w") as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        for line in data:
            writer.writerow(line)


def ConfigSectionMap(section):
    Config = configparser.ConfigParser()
    dir_path = os.path.dirname(os.path.realpath(__file__))
    config_path = dir_path.split('xml_data')[0] + 'db.ini'
    Config.read(config_path)
    dict1 = {}
    options = Config.options(section)
    for option in options:
        try:
            dict1[option] = Config.get(section, option)
            if dict1[option] == -1:
                DebugPrint("skip: %s" % option)
        except:
            print("exception on %s!" % option)
            dict1[option] = None
    return dict1
