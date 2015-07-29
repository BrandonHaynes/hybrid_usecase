#!/usr/bin/env python
import sys
import subprocess
import argparse
import logging
import re
from urlparse import urlparse
import urllib
import requests
import json
import SimpleHTTPServer
import SocketServer
from myria import MyriaConnection

class DemoTCPServer(SocketServer.TCPServer):
    def __init__(self, address, handler, arguments):
        SocketServer.TCPServer.__init__(self, address, handler)

        self.arguments = arguments
        self.logger = logging.getLogger('DemoServer')

class DemoHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
    expression = r'^MYRIA\((?P<query>.*)\)$'

    def do_GET(self):
        match = re.search(self.expression, self.querystring.get('query', ''))
        if match:
            query = match.group('query')
            if query:
                connection = MyriaConnection(rest_url=self.server.arguments.myria_url, execution_url=self.server.arguments.myria_web_url)
                response = connection.execute_program(urllib.unquote(query).replace('+', ' ').replace('plus', '+'))

                self.send_response(200)
                self.send_access_control_headers()
                self.end_headers()
                self.wfile.write(json.dumps(response))

                #request = requests.post(self.server.arguments.myria_web_url + '/execute?language=myrial', data={'query': urllib.unquote(query).replace('+', ' ').replace('plus', '+')})

                #self.send_response(request.status_code)
                #self.send_access_control_headers()
                #self.end_headers()
                #self.wfile.write(request.text)
                return

        self.send_response(500)
        self.end_headers()

    def do_POST(self):
        length = int(self.headers['content-length'])
        query = self.rfile.read(length)
        values = dict((pair.split("=") + [''])[:2] for pair in query.split("&"))
        match = re.search(self.expression, values.get('query', ''))
        if match:
            query = match.group('query')
            if query:
                connection = MyriaConnection(rest_url=self.server.arguments.myria_url, execution_url=self.server.arguments.myria_web_url)
                response = connection.execute_program(urllib.unquote(query).replace('+', ' ').replace('plus', '+'))

                self.send_response(200)
                self.send_access_control_headers()
                self.end_headers()
                self.wfile.write(json.dumps(response))

                #request = requests.post(self.server.arguments.myria_web_url + '/execute?language=myrial', data={'query': urllib.unquote(query).replace('+', ' ').replace('plus', '+')})

                #self.send_response(request.status_code)
                #self.send_access_control_headers()
                #self.end_headers()
                #self.wfile.write(request.text)
                return

        self.send_response(500)
        self.end_headers()

    @property
    def querystring(self):
        query = urlparse(self.path).query
        return dict((pair.split("=") + [''])[:2] for pair in query.split("&"))

    def send_access_control_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")

def parse_arguments(arguments):
    parser = argparse.ArgumentParser(description='Launch hybrid demo webserver')
    parser.add_argument('--myria-url', type=str, dest='myria_url', required=True, help='Myria REST URL')
    parser.add_argument('--myria-web-url', type=str, dest='myria_web_url', required=True, help='Myria Web URL')

    parser.add_argument('--port', type=int, default=8080, help='Webserver port number')

    return parser.parse_args(arguments)

if __name__ == "__main__":
    arguments = parse_arguments(sys.argv[1:])
    print arguments
    logging.getLogger().setLevel(logging.DEBUG)

    DemoTCPServer(('0.0.0.0', arguments.port),
                    DemoHandler,
                    arguments).serve_forever()