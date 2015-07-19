#!/usr/bin/env python

""" Create a SciDB configuration file """

import os
import sys
import getpass
import argparse
import textwrap


INDENT = 28


def indent(line, i, amount=INDENT):
    return (' ' * amount if i else '') + line

def get_configuration(**kwargs):
    """ Generates a SciDB configuration file with the given properties """
    kwargs['workers'] = '\n'.join(
        indent('server-{}={},{}'.format(
            i,
            name,
            kwargs['workers'] - (0 if i else 1)), i) \
          for i, name in enumerate(kwargs['nodes']))

    return textwrap.dedent("""\
                            [{database_name}]
                            {workers}
                            db_user={database_user}
                            db_passwd={database_password}
                            pg-port={database_port}
                            install_root={path}
                            pluginsdir={plugins_path}
                            logconf={log4xx_config_filename}
                            base-path={data_path}
                            base-port={base_port}
                            redundancy={redundancy}
                            {metadata}""".format(**kwargs)).strip()


def main(argv):
    """ Argument parsing wrapper for generating a SciDB configuration file """
    parser = argparse.ArgumentParser(
        description='Create a SciDB configuration file')

    parser.add_argument(
        'nodes', type=str, nargs='+',
        help='One or more worker hostnames')

    parser.add_argument(
        '--workers', type=int,
        default=2, help='Number of workers per node')

    parser.add_argument(
        '--path', type=str, required=True,
        help='Path to SciDB installation')
    parser.add_argument(
        '--plugins-path', type=str,
        default=None, help='Path to SciDB plugin libraries')
    parser.add_argument(
        '--data-path', type=str,
        default=None, help='Path to SciDB data')
    parser.add_argument(
        '--log4xx-config-filename', type=str,
        default=None, help='Qualified filename of Log4cxx configuration')

    parser.add_argument(
        '--database-name', type=str,
        default=getpass.getuser(), help='Name of the Postgres catalog')
    parser.add_argument(
        '--database-user', type=str,
        default=getpass.getuser(), help='Username for connecting to the Postgres catalog')
    parser.add_argument(
        '--database-password', type=str, required=True,
        help='Password for connecting to the Postgres catalog')
    parser.add_argument(
        '--database-port', type=int,
        default=5432, help='Port used to connect to the Postgres catalog')

    parser.add_argument(
        '--base-port', type=int, default=1250,
        help='Base port for SciDB workers')
    parser.add_argument(
        '--redundancy', type=int, default=0,
        help='SciDB chunk reduncancy across nodes')

    parser.add_argument(
        '--metadata', type=str, default='',
        help='List of additional comma-separated configuration key/value pairs (e.g., "foo=bar,baz=qux")')

    arguments = parser.parse_args(argv[1:])
    arguments.plugins_path = os.path.join(arguments.path, 'lib', 'scidb', 'plugins')
    arguments.data_path = os.path.join(arguments.path, 'data')
    arguments.log4xx_config_filename = os.path.join(arguments.path, 'share', 'scidb', 'log4cxx.properties')
    arguments.metadata = '\n'.join(indent(line, i) for i, line in enumerate(arguments.metadata.split(',')))

    print get_configuration(**vars(arguments))

if __name__ == "__main__":
    main(sys.argv)
