#!/usr/bin/env python
import sys
import subprocess
import argparse
import logging
from urlparse import urlparse
import SimpleHTTPServer
import SocketServer

class DemoTCPServer(SocketServer.TCPServer):
    def __init__(self, address, handler, arguments):
        SocketServer.TCPServer.__init__(self, address, handler)

        self.arguments = arguments
        self.logger = logging.getLogger('DemoServer')

class DemoHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
    PASSTHROUGH = ['/index.html', '/myria.html', '/scidb.html', '/hybrid-csv.html', '/hybrid-binary.html',
                   '/index.js', '/workflow.js', '/bullet.js', '/cola.min.js', '/cola.js',
                   '/cola.min.js.map',
                   '/bullet.css',
                   '/bullets.json', '/graph.json']

    def do_GET(self):
        if self.path in self.PASSTHROUGH:
            self.output(self.path.strip('/'))
        elif self.path.startswith('/execute.html'):
            self.execute(self.querystring.get('system', '').lower())
        else:
            self.send_response(404)
            self.end_headers()

    def execute(self, system):
        if system == 'myria':
            self.send_response(200)
            self.end_headers()
            self.wfile.write(subprocess.check_output(['starcluster', 'sshmaster', self.server.arguments.cluster_name,
                                     "python myria_only.py {patients} {vector_size} --url {url}".format(
                                        patients=self.server.arguments.patients,
                                        vector_size=self.server.arguments.vector_size,
                                        url=self.server.arguments.myria_url)],
                                    stderr=subprocess.STDOUT))
        elif system == 'scidb':
            self.send_response(200)
            self.end_headers()
            self.wfile.write(subprocess.check_output(['starcluster', 'sshmaster', self.server.arguments.cluster_name,
                                     "python scidb_only.py {patients} {vector_size} --url {url}".format(
                                        patients=self.server.arguments.patients,
                                        vector_size=self.server.arguments.vector_size,
                                        url=self.server.arguments.scidb_url)],
                                    stderr=subprocess.STDOUT))
        elif system == 'hybrid-csv':
            self.send_response(200)
            self.end_headers()
            self.wfile.write(subprocess.check_output(['starcluster', 'sshmaster', self.server.arguments.cluster_name,
                                     "python hybrid.py {patients} {vector_size} {scidb_workers} --myria-url {url}".format(
                                        patients=self.server.arguments.patients,
                                        vector_size=self.server.arguments.vector_size,
                                        scidb_workers=self.server.arguments.scidb_workers,
                                        url=self.server.arguments.myria_url)],
                                    stderr=subprocess.STDOUT))
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write('System not found.')

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
    parser.add_argument('cluster_name', type=str, help='Name of cluster on which to execute queries')
    parser.add_argument('patients', type=int, default=600, help='Number of patients in test')
    parser.add_argument('vector_size', type=int, default=256, help='Size of input vector')
    parser.add_argument('scidb_workers', type=int, help='Number of SciDB workers')

    parser.add_argument('--myria-url', type=str, dest='myria_url', default='http://localhost:8080', help='SciDB Shim URL')
    parser.add_argument('--scidb-url', type=str, dest='scidb_url', default='http://localhost:8080', help='SciDB Shim URL')
    parser.add_argument('--port', type=int, default=8752, help='Webserver port number')

    return parser.parse_args(arguments)

if __name__ == "__main__":
    arguments = parse_arguments(sys.argv[1:])

    logging.getLogger().setLevel(logging.DEBUG)

    DemoTCPServer(('0.0.0.0', arguments.port),
                    DemoHandler,
                    arguments).serve_forever()