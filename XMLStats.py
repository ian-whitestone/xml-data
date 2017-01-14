import sys

import urllib.request
import urllib.parse
import urllib.error

import gzip
import json
from datetime import datetime
from datetime import timedelta
import dateutil.parser
# import socket
# import ssl
import time
import os
import general_utils as Ugen
import io

# https://github.com/xmlstats/example-python/blob/master/roster.py

# Replace with your access token
XMLLogin = Ugen.ConfigSectionMap('XMLStats')
access_token = XMLLogin['token']


# Replace with your bot name and email/website to contact if there is a problem
# e.g., "mybot/0.1 (https://erikberg.com/)"
user_agent = XMLLogin['email']

# Some problems have been reported that Python 2.x fails to negotiate the
# correct SSL protocol when connecting over HTTPS. This code forces
# Python to use TLSv1.
# More information and code from http://bugs.python.org/issue11220


def main(sport,method,parameters,game_id=False):
    # set the API method, format, and any parameters
    host = "erikberg.com"
    data_format = "json"

    # Pass method, format, and parameters to build request url
    url = build_url(host, sport, method, data_format, parameters, game_id)
    data, xmlstats_remaining, xmlstats_reset=http_get(url)
    if xmlstats_remaining == 0:
        delta = (datetime.fromtimestamp(xmlstats_reset) - datetime.now()).total_seconds()
        print('Reached rate limit. Waiting {} seconds to make new '
              'request...'.format(int(delta)))
        time.sleep(delta+1)

    if data:
        return json.loads(data.decode('UTF-8'))
    else:
        return False


def http_get(url):

    req = urllib.request.Request(url)
    # Set Authorization header
    req.add_header('Authorization', 'Bearer ' + access_token)
    # Set user agent
    req.add_header('User-agent', user_agent)
    # Tell server we can handle gzipped content
    req.add_header('Accept-encoding', 'gzip')


    try:
        response = urllib.request.urlopen(req)
    except urllib.error.HTTPError as err:
        # If error is of type application/json, it will be an XmlstatsError
        # see https://erikberg.com/api/objects/xmlstats-error
        if err.headers.get('content-type') == 'application/json':
            data = json.loads(err.read().decode('UTF-8'))
            reason = data['error']['description']
        else:
            reason = err.read()
        print('Server returned {} error code!\n{}'.format(err.code, reason))
        return False,1,False
    except urllib.error.URLError as err:
        print('Error retrieving file: {}'.format(err.reason))
        return False,1,False

    data = None
    headers = response.info()
    xmlstats_reset = int(headers.get('xmlstats-api-reset'))
    xmlstats_remaining = int(headers.get('xmlstats-api-remaining'))

    if response.info().get('Content-encoding') == 'gzip':
        buf = io.BytesIO(response.read())
        f = gzip.GzipFile(fileobj=buf)
        data = f.read()
    else:
        data = response.read()
    return data, xmlstats_remaining, xmlstats_reset

# See https://erikberg.com/api/methods Request URL Convention for
# an explanation
def build_url(host, sport, method, data_format, parameters, game_id = False):
    path = "/".join(filter(None, (sport, method, game_id)));
    url = "https://" + host + "/" + path + "." + data_format
    print (url)
    if parameters:
        paramstring = urllib.parse.urlencode(parameters)
        url = url + "?" + paramstring
    return url
