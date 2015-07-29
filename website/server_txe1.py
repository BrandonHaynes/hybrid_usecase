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
    PASSTHROUGH = ['/index.html', '/screen.html', '/myria.html', '/scidb.html', '/hybrid-csv.html', '/hybrid-binary.html',
                   '/index.js', '/workflow.js', '/bullet.js', '/cola.min.js', '/cola.js', '/screen.js',
                   '/cola.min.js.map',
                   '/bullet.css', '/screen.css',
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
        prefix = ['ssh', self.server.arguments.login_node]

        if system == 'iquery':
            self.send_response(200)
            self.end_headers()
            command = ("""ssh {coordinator} "{path}/bin/iquery -a -p {port} -q '{query}'" """.format(
                coordinator=self.server.arguments.coordinator,
                path=self.server.arguments.path,
                port=self.server.arguments.port,
                query=urlparse(self.path).query))

        elif system == 'myria':
            self.send_response(200)
            self.end_headers()
            command = (" ""PYTHONPATH={install_path}/../myria-python:$PYTHONPATH ; "
                       "python {path}/myria_only.py {patients} {vector_size} "
                            "--url {url} "
                            "--execution-url {web_url} "
                            "--install-path {install_path} "
                            "--deployment-file {install_path}/../deploy/deployment.config"" "
                            .format(
                                        patients=self.server.arguments.patients,
                                        vector_size=self.server.arguments.vector_size,
                                        path=self.server.arguments.path,
                                        url=self.server.arguments.myria_url,
                                        web_url=self.server.arguments.myria_web_url,
                                        install_path=self.server.arguments.myria_path))
        elif system == 'scidb':
            self.send_response(200)
            self.end_headers()
            command = ("python {path}/scidb_only.py {patients} {vector_size} "
                           "--url {url} "
                           "--scidb-bin {scidb_path}/bin/scidb.py "
                           "--scidb-iquery {scidb_path}/bin/iquery "
                           "--scidb-port {scidb_port} "
                           "--scidb-name {scidb_name}".format(
                                        patients=self.server.arguments.patients,
                                        vector_size=self.server.arguments.vector_size,
                                        path=self.server.arguments.path,
                                        url=self.server.arguments.scidb_url,
                                        scidb_path=self.server.arguments.scidb_path,
                                        scidb_port=self.server.arguments.scidb_port,
                                        scidb_name=self.server.arguments.scidb_name))
        elif system == 'hybrid-csv':
            self.send_response(200)
            self.end_headers()
            command = "python /root/hybrid.py {patients} {vector_size} {scidb_workers} --myria-url {url}".format(
                                        patients=self.server.arguments.patients,
                                        vector_size=self.server.arguments.vector_size,
                                        scidb_workers=self.server.arguments.scidb_workers,
                                        url=self.server.arguments.myria_url)
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write('System not found.')
            command = None

        if command:
            # Injection vulnerability!
            self.wfile.write(subprocess.check_output(prefix + ["ssh " + self.server.arguments.coordinator + " " + command],
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
    parser.add_argument('coordinator', type=str, help='Name of coordinator node on which to execute queries')
    parser.add_argument('patients', type=int, default=600, help='Number of patients in test')
    parser.add_argument('vector_size', type=int, default=256, help='Size of input vector')
    parser.add_argument('scidb_workers', type=int, help='Number of SciDB workers')

    parser.add_argument('--login-node', type=str, dest='login_node', default='txe1-login.mit.edu', help='Name of cluster login node')
    parser.add_argument('--path', type=str, dest='path', default='~/hybrid_usecase', help='Name of cluster login node')

    parser.add_argument('--myria-url', type=str, dest='myria_url', required=True, help='Myria REST URL')
    parser.add_argument('--myria-web-url', type=str, dest='myria_web_url', required=True, help='Myria Web URL')
    parser.add_argument('--myria-path', type=str, dest='myria_path', required=True, help='Root path of Myria installation')

    parser.add_argument('--scidb-url', type=str, dest='scidb_url', required=True, help='SciDB Shim URL')
    parser.add_argument('--scidb-path', type=str, dest='scidb_path', required=True, help='Root path of SciDB installation')
    parser.add_argument('--scidb-port', type=str, dest='scidb_port', required=True, help='SciDB coordinator port')
    parser.add_argument('--scidb-name', type=str, dest='scidb_name', required=True, help='SciDB instance name')

    parser.add_argument('--port', type=int, default=8752, help='Webserver port number')

    return parser.parse_args(arguments)

if __name__ == "__main__":
    arguments = parse_arguments(sys.argv[1:])
    print arguments
    logging.getLogger().setLevel(logging.DEBUG)

    DemoTCPServer(('0.0.0.0', arguments.port),
                    DemoHandler,
                    arguments).serve_forever()