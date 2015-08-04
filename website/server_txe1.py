#!/usr/bin/env python
import sys
import subprocess
import argparse
import logging
from urlparse import urlparse
import urllib
import SimpleHTTPServer
import SocketServer

class DemoTCPServer(SocketServer.TCPServer):
    def __init__(self, address, handler, arguments):
        SocketServer.TCPServer.__init__(self, address, handler)

        self.arguments = arguments
        self.logger = logging.getLogger('DemoTCPServer')

class DemoHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
    PASSTHROUGH = ['/index.html', '/screen.html', '/myria.html', '/scidb.html', '/hybrid-csv.html', '/hybrid-binary.html',
                   '/js/index.js', '/js/workflow.js', '/js/bullet.js', '/js/cola.min.js', '/js/cola.js', '/js/screen.js', '/js/jquery-2.1.4.js', '/js/d3.min.js', '/js/bootstrap.min.js', '/js/cola.min.js'
                   '/cola.min.js.map',
                   '/css/bullet.css', '/css/screen.css', '/css/bootstrap.min.css', '/css/bootstrap-theme.min.css',
                   '/bullets.json', '/graph.json',
                   '/queries/federated.txt', '/queries/myria.txt', '/queries/scidb.txt']

    def do_GET(self):
        if self.path in self.PASSTHROUGH:
            self.output(self.path.strip('/'))
        elif self.path.startswith('/iquery'):
            self.execute('iquery')
        else:
            self.send_response(404)
            self.end_headers()

    def execute(self, system):
        prefix = ['ssh', self.server.arguments.scidb_node]

        if system == 'iquery':
            self.send_response(200)
            self.end_headers()
            command = ("""{path}/bin/iquery -a -n -p {port} -q "{query}" """.format(
                path=self.server.arguments.scidb_path,
                port=self.server.arguments.scidb_port,
                query=urllib.unquote(urlparse(self.path).query).replace('csv+', 'csvplus').replace('+', ' ').replace('csvplus', 'csv+').replace("\\'", "'")))
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write('System not found.')
            command = None

        if command:
            self.wfile.write(subprocess.check_output(prefix + [command],
                             stderr=subprocess.STDOUT))


    def output(self, filename):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(open(filename).read())

    def redirect(self, path):
        self.send_response(302)
        self.send_access_control_headers()
        self.send_header('Location', path)
        self.end_headers()

    @property
    def querystring(self):
        query = urlparse(self.path).query
        return dict(pair.split("=") for pair in query.split("&"))

def parse_arguments(arguments):
    parser = argparse.ArgumentParser(description='Launch hybrid demo webserver')

    parser.add_argument('--scidb-node', type=str, dest='scidb_node', default='localhost', help='Name of node with iquery')
    parser.add_argument('--scidb-port', type=int, dest='scidb_port', default=1260, help='SciDB coordinator port')
    parser.add_argument('--scidb-path', type=str, dest='scidb_path', help='Root path of SciDB installation')

    parser.add_argument('--port', type=int, default=8751, help='Webserver port number')

    return parser.parse_args(arguments)

if __name__ == "__main__":
    arguments = parse_arguments(sys.argv[1:])
    print arguments
    logging.getLogger().setLevel(logging.DEBUG)

    DemoTCPServer(('0.0.0.0', arguments.port),
                    DemoHandler,
                    arguments).serve_forever()