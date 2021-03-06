#!/usr/bin/env python
import os
import sys
import subprocess
import argparse
import json
import logging
from urlparse import urlparse, parse_qs
import urllib
import SimpleHTTPServer
import SocketServer
from myria import MyriaConnection, MyriaRelation

class DemoTCPServer(SocketServer.TCPServer):
    def __init__(self, address, handler, arguments):
        SocketServer.TCPServer.__init__(self, address, handler)

        self.arguments = arguments
        self.logger = logging.getLogger('DemoTCPServer')

class DemoHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
    PASSTHROUGH = ['/index.html', '/screen.html', '/myria.html', '/scidb.html', '/hybrid-csv.html', '/hybrid-binary.html',
                   '/details/myria.html', '/details/scidb.html', '/details/hybrid-text.html', '/details/hybrid-binary.html', '/backup.html',
                   '/js/index.js', '/js/workflow.js', '/js/bullet.js', '/js/cola.min.js', '/js/cola.js', '/js/screen.js', '/js/jquery-2.1.4.js', '/js/d3.min.js', '/js/bootstrap.min.js', '/js/cola.min.js', '/js/run_prettify.js', '/js/backup.js',
                   '/cola.min.js.map',
                   '/css/bullet.css', '/css/screen.css', '/css/bootstrap.min.css', '/css/bootstrap-theme.min.css', '/css/navbar-custom.css',
                   '/bullets.json', '/graph.json', '/queries/mapping.json',
                   '/queries/federated.txt', '/queries/myria.txt', '/queries/scidb.txt',
                   '/images/transfer-text.png', '/images/transfer-binary.png', '/images/hybrid-execution.png', '/images/scidb-execution.png', '/images/myria-execution.png', '/images/bracket.png', '/images/automatically.png', '/images/transfer-detail.png', '/img/bigdawglogo.png',
                   '/favicon.ico']

    def do_GET(self):
        if self.path in self.PASSTHROUGH:
            self.output(self.path.strip('/'))
        elif self.path.startswith('/iquery'):
            self.execute('iquery', urlparse(self.path).query)
        elif self.path.startswith('/dataset'):
            self.execute('myria', urlparse(self.path).query)
        elif self.path == '/':
            self.send_response(301)
            self.send_header('Location', '/screen.html')
            self.end_headers()
        else:
            self.send_response(404)
            self.end_headers()


    def do_POST(self):
        if '/iquery' in self.path:
            length = int(self.headers.getheader('content-length'))
            data = self.rfile.read(length)
            fields = parse_qs(data)
            self.execute('iquery', fields['query'][0])
        elif '/prepare' in self.path:
            self.execute('restart', None)
        elif '/final' in self.path:
            self.execute('restart', None)
        else:
            self.send_response(404)
            self.end_headers()


    def do_OPTIONS(self):
        if self.path.startswith('/iquery'):
            self.send_response(200)
            self.send_header('Allow', 'GET,POST,OPTIONS')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Access-Control-Allow-Methods', 'GET,POST')
            self.send_header('Access-Control-Allow-Headers', 'Content-Type')
            self.end_headers()
        elif self.path.startswith('/dataset'):
            self.send_response(200)
            self.send_header('Allow', 'GET,OPTIONS')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Access-Control-Allow-Methods', 'GET')
            self.send_header('Access-Control-Allow-Headers', 'Content-Type')
            self.end_headers()
        elif self.path.startswith('/prepare'):
            self.send_response(200)
            self.send_header('Allow', 'POST,OPTIONS')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Access-Control-Allow-Methods', 'POST')
            self.send_header('Access-Control-Allow-Headers', 'Content-Type')
            self.end_headers()
        else:
            self.send_response(404)
            self.end_headers()

    def execute(self, system, data):
        if system == 'iquery':
            self.send_response(200)
            self.end_headers()
            command = ['{path}/bin/iquery'.format(path=self.server.arguments.scidb_path),
                       '-anp', str(self.server.arguments.scidb_port),
                       '-q', (urllib.unquote(data)
                                    .replace('csv+', 'csvplus')
                                    .replace('+', ' ')
                                    .replace('csvplus', 'csv+')
                                    .replace("\\'", "'")
                                    .replace('\n', ' ')
                                    .replace('"', '\\"'))]
            self.wfile.write(subprocess.check_output(command,
                             stderr=subprocess.STDOUT))
        elif system == 'myria':
            self.send_response(200)
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            connection = MyriaConnection(rest_url=self.server.arguments.myria_url)
            relation = MyriaRelation(data, connection=connection)
            self.wfile.write(json.dumps(relation.to_dict()))

        elif system == 'restart':
            myria_path = self.server.arguments.myria_path
            body = subprocess.check_output([
                './kill_all_java_processes.py', os.path.join(myria_path, 'deploy/deployment.config')],
                cwd=os.path.join(myria_path, 'stack/myria/myriadeploy'),
                stderr=subprocess.STDOUT)
            body += subprocess.check_output([
                './launch_cluster.sh', os.path.join(myria_path, 'deploy/deployment.config')],
                cwd=os.path.join(myria_path, 'stack/myria/myriadeploy'),
                stderr=subprocess.STDOUT)


            scidb_path = self.server.arguments.scidb_path
            body +=subprocess.check_output([
                'bin/scidb.py', 'stop_all', 'bhaynes', '{}/etc/config.ini'.format(scidb_path)],
                cwd=scidb_path,
                stderr=subprocess.STDOUT)
            body += subprocess.check_output([
                'bin/scidb.py', 'start_all', 'bhaynes', '{}/etc/config.ini'.format(scidb_path)],
                cwd=scidb_path,
                stderr=subprocess.STDOUT)
            body += subprocess.check_output([
                'bin/iquery', '-anp', str(self.server.arguments.scidb_port), '-q',
                'scan(SciDB__Demo__Vectors)'],
                cwd=scidb_path,
                stderr=subprocess.STDOUT)

            self.send_response(200)
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(body)

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

    parser.add_argument('--scidb-node', type=str, dest='scidb_node', default='localhost', help='Name of node with iquery')
    parser.add_argument('--scidb-port', type=int, dest='scidb_port', default=1260, help='SciDB coordinator port')
    parser.add_argument('--scidb-path', type=str, dest='scidb_path', default='/state/partition1/scidb-bhaynes', help='Root path of SciDB installation')

    parser.add_argument('--myria-url', type=str, dest='myria_url', default='http://localhost:8753', help='Myria REST URL')
    parser.add_argument('--myria-path', type=str, dest='myria_path', default='/state/partition1/myria_bhaynes', help='Myria REST URL')

    parser.add_argument('--port', type=int, default=8751, help='Webserver port number')

    return parser.parse_args(arguments)

if __name__ == "__main__":
    arguments = parse_arguments(sys.argv[1:])
    print arguments
    logging.getLogger().setLevel(logging.DEBUG)

    DemoTCPServer(('0.0.0.0', arguments.port),
                    DemoHandler,
                    arguments).serve_forever()