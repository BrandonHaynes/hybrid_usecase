import sys
import subprocess
import time
import math
import tempfile
import argparse
import json
import timeit
import scidbpy
from myria import *


def time_scidb(query, arguments, n=1):
    return timeit.Timer(lambda: execute_scidb(query, arguments)).timeit(number=n)

def time_myria(plan, arguments, n=1):
    return execute_myria(plan, arguments)

###########################################################################
# Myria
###########################################################################

MYRIA_QUERY = """
const test_vector_id: {id};
const bins: {bins};
def idf(w_ij, w_ijN, N): log(N / w_ijN) * w_ij;

{symbols}

------------------------------------------------------------------------------------
-- IDF
------------------------------------------------------------------------------------
ids = distinct([from symbols emit id]);
N = [from ids emit count(*) as N];
frequencies = [from symbols emit value, index, count(*) as frequency];

tfv = [from symbols, frequencies, N
       where symbols.value = frequencies.value
       emit id, index, idf(value, frequency, N) as value];

------------------------------------------------------------------------------------
-- Conditioning
------------------------------------------------------------------------------------
moments = [from tfv emit id,
                         avg(value) as mean,
                         -- Sample estimator
                         sqrt((stdev(value)*stdev(value)*count(value))/(count(value)-1)) as std];
conditioned_tfv = [from tfv, moments
                   where tfv.id = moments.id
                   emit id, index, value as v, mean, std, (value - mean) / std as value];
sum_squares = [from conditioned_tfv
               emit id, sum(pow(value, 2)) as sum_squares];

------------------------------------------------------------------------------------
-- k-NN
------------------------------------------------------------------------------------

test_vector = [from conditioned_tfv where id = test_vector_id emit *];

products = [from test_vector as x,
                 conditioned_tfv as y
                where x.index = y.index
                emit y.id as id, sum(x.value * y.value) as product];

correlations = [from products, sum_squares
                where products.id = sum_squares.id
                emit products.id as id, product / sum_squares as rho];

sink(correlations);
"""

def execute_myria(plan, arguments):
    query = MyriaQuery.submit_plan(plan, connection=arguments.myria_connection)
    try:
      query.wait_for_completion(timeout=3600)
    except AttributeError as e:
      if query.status != 'SUCCESS': raise e
    return arguments.myria_connection.get_query_status(query.query_id)['elapsedNanos'] / 1E9 \
                     if query.status == 'SUCCESS' else None

def myria_plan(arguments):
    return arguments.myria_connection.compile_program(
        MYRIA_QUERY.format(
            symbols=myria_symbols(arguments),
            id=arguments.test_id,
            bins=arguments.bins))

def myria_symbols(arguments):
    return myria_loads(arguments) +       \
           myria_unions(arguments) +      \
           'store(symbols, symbols);\n' + \
           'symbols = scan(symbols);'

def myria_loads(arguments):
    return ''.join(['symbols{w}x{i} = load("{filename}", csv(schema(id:int, index:int, value:int)));\n'.format(
                      filename=myria_filename(arguments, w, i),
                      w=w,
                      i=i) \
                        for i in xrange(1, arguments.iterations+1) \
                        for w in xrange(arguments.scidb_workers)])

def myria_unions(arguments):
    return 'symbols = {};\n'.format(
                 ' + '.join(['symbols{}x{}'.format(w, i) \
                             for w in xrange(arguments.scidb_workers) \
                             for i in xrange(1, arguments.iterations+1)]))

def myria_filename(arguments, worker, index):
    return 'file://{path}/{worker}/{filename}'.format(
        path=arguments.intermediate_path,
        worker=worker,
        filename=get_transform_name(arguments, index))

def ensure_symbols_relation(arguments):
    query = MyriaQuery.submit("""
      singleton_symbols = empty(id:int, index:int, value:int);
      shuffled_symbols = [from singleton_symbols emit id, index, value, count(*)];
      symbols = [from shuffled_symbols emit id, index, value];
      store(symbols, symbols);
      """, connection=arguments.myria_connection)
    try:
      query.wait_for_completion(timeout=3600)
    except AttributeError as e:
      if query.status != 'SUCCESS': raise e
    return arguments.myria_connection.get_query_status(query.query_id)['elapsedNanos'] / 1E9 \
                     if query.status == 'SUCCESS' else None


###########################################################################
# SciDB
###########################################################################

def execute_scidb(query, arguments):
    try:
        subprocess.check_output([arguments.scidb_iquery, '-p', str(arguments.scidb_port), '-anq', query], stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        print e.cmd
        print e.output
        raise e

def build_input(arguments):
    print 'Creating input vectors (mode=%s)' % arguments.input_mode
    if arguments.input_mode.lower() == 'deterministic':
        arguments.input_array = (
            scidbpy.connect(arguments.scidb_url)
                   .afl.build('<value: double>[id=0:{},{},{}, time=0:{},{},{}]'.format(
                                  arguments.patients-1, arguments.chunk_patients, arguments.overlap_patients,
                                  arguments.vector_size-1, arguments.chunk_vectors, arguments.overlap_vectors),
                              '(double(id+1) / (time+1)) / %f - 1' % (arguments.patients/2))
                   .eval()
                   .name)
    else:
        arguments.input_array = (
            scidbpy.connect(arguments.scidb_url)
                   .random((arguments.patients, arguments.vector_size),
                           chunk_size=(arguments.chunk_patients, arguments.chunk_vectors),
                           dim_names=['id', 'time'])
                   .attribute_rename('f0', 'value')
                   .eval()
                   .name)

def transform_arrays(arguments):
    return '\n'.join(['''create temp array {name}
                           <value: double null, bucket:int64 null>
                           [id=0:{patients},{chunk_patients},{overlap_patients},
                            time=0:{vector_size},{chunk_vectors},{overlap_vectors}];'''.format(
                                name=get_transform_name(arguments, i+1),
                                patients=arguments.patients-1,
                                chunk_patients=arguments.chunk_patients,
                                overlap_patients=arguments.overlap_patients,
                                vector_size=(arguments.vector_size / 2**(i+1))-1,
                                chunk_vectors=arguments.chunk_vectors,
                                overlap_vectors=arguments.overlap_vectors)
                            for i in xrange(arguments.iterations)])

def output_arrays(arguments):
    return '\n'.join(['''create temp array out_{name}
                           <value:int64 null>
                           [id=0:{patients},{chunk_patients},{overlap_patients},
                            bucket=0:{bins},{chunk_bins},{overlap_bins}];'''.format(
                                name=get_transform_name(arguments, i+1),
                                patients=arguments.patients-1,
                                chunk_patients=arguments.chunk_output,
                                overlap_patients=arguments.overlap_patients,
                                bins=arguments.bins-1,
                                chunk_bins=arguments.chunk_bins,
                                overlap_bins=arguments.overlap_bins)
                            for i in xrange(arguments.iterations)])

def scidb_query(arguments):
    query = (transform_arrays(arguments) +
             output_arrays(arguments))

    for i in xrange(arguments.iterations):
        query += """save(
                      redimension(
                        store(
                          regrid(
                            scan({current}),
                            1, 2,
                            avg(value), bin{range}(value)),
                            {next}),
                        {out},
                        signed_count(bucket) as value),
                      '{filename}', -1, 'csv+');\n""".format(
            range=2**i,
            current=get_transform_name(arguments, i),
            next=get_transform_name(arguments, i+1),
            out='out_' + get_transform_name(arguments, i+1),
            filename='%s/%s' % (arguments.output_path, get_transform_name(arguments, i+1)))
    return query


def get_transform_name(arguments, index):
    return get_name(arguments, arguments.transform_format % index) if index else arguments.input_array


def get_name(arguments, name):
    return name + '_' + arguments.suffix


def restart_scidb(arguments):
    print 'Restarting SciDB'

    if arguments.fast:
      return

    try:
        subprocess.check_output([arguments.scidb_bin, 'stopall', arguments.scidb_name], stderr=subprocess.STDOUT)
        subprocess.check_output([arguments.scidb_bin, 'startall', arguments.scidb_name], stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        print e.cmd
        print e.output
        raise e


def warm_array(arguments):
    print 'Prescanned array'

    if arguments.fast:
      return

    try:
        subprocess.check_output([arguments.scidb_iquery, '-p', str(arguments.scidb_port), '-anq', 'scan({})'.format(arguments.input_array)], stderr=subprocess.STDOUT)
        subprocess.check_output([arguments.scidb_iquery, '-p', str(arguments.scidb_port), '-anq', "load_library('bin')"], stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        print e.cmd
        print e.output
        raise e

###########################################################################

def parse_arguments(arguments):
    parser = argparse.ArgumentParser(description='Execute SciDB-only test')
    parser.add_argument('patients', type=int, help='Number of patients to evaluate')
    parser.add_argument('vector_size', type=int, help='Size of input vectors for each patient')
    parser.add_argument('scidb_workers', type=int, help='Number of SciDB workers')

    parser.add_argument('--bins', type=int, default=10, help='Number of histogram bins')
    parser.add_argument('--scidb-url', type=str, dest='scidb_url', default='http://localhost:8080', help='SciDB Shim URL')
    parser.add_argument('--myria-url', type=str, dest='myria_url', default='http://localhost:8753', help='Myria REST URL')
    parser.add_argument('--myria-web-url', type=str, dest='myria_web_url', default='http://localhost:80', help='Myria Web URL')

    parser.add_argument('--test-id', dest='test_id', type=int, default=1, help='Index of test patient for k-NN computation')
    parser.add_argument('--input-mode', dest='input_mode', type=str, default='random', choices=['deterministic', 'random'], help='Mode of automatically generated input')

    parser.add_argument('--scidb-bin', dest='scidb_bin', type=str, default='/opt/scidb/14.12/bin/scidb.py', help='Path of scidb.py')
    parser.add_argument('--scidb-iquery', dest='scidb_iquery', type=str, default='/opt/scidb/14.12/bin/iquery', help='Path of iquery')
    parser.add_argument('--scidb-name', dest='scidb_name', type=str, default='mydb', help='Name of SciDB database')
    parser.add_argument('--scidb-port', dest='scidb_port', type=int, default='1239', help='Coordinator port for SciDB')

    parser.add_argument('--chunk-patients', dest='chunk_patients', type=int, default=1, help='Chunk size for patient array')
    parser.add_argument('--chunk-vectors', dest='chunk_vectors', type=int, default=None, help='Chunk size for input vectors')
    parser.add_argument('--chunk-bins', dest='chunk_bins', type=int, default=2**32, help='Chunk size for histogram bins')
    parser.add_argument('--chunk-output', dest='chunk_output', type=int, default=None, help='Chunk size for output aggregates')

    parser.add_argument('--overlap-vectors', dest='overlap_vectors', type=int, default=0, help='Array overlap for input vectors')
    parser.add_argument('--overlap-patients', dest='overlap_patients', type=int, default=0, help='Array overlap for patient array')
    parser.add_argument('--overlap-bins', dest='overlap_bins', type=int, default=0, help='Array overlap for histogram bins')

    parser.add_argument('--output-path', dest='output_path', type=str, default='out', help='SciDB worker-relative directory to store input relation partitions')
    parser.add_argument('--intermediate-path', dest='intermediate_path', type=str, default='/home/scidb', help='Common NFS path across both SciDB and Myria for intermediate results')

    parser.add_argument('--fast', action='store_true', help='Favor speed over fair benchmark results')

    arguments = parser.parse_args(arguments)
    arguments.chunk_vectors = arguments.chunk_vectors or arguments.vector_size
    arguments.chunk_output = arguments.chunk_output or min(arguments.vector_size, 2**24)
    arguments.transform_format = 'transform_%d'
    arguments.suffix = str(int(time.time()))
    arguments.iterations = int(math.log(arguments.vector_size, 2))
    arguments.myria_connection = MyriaConnection(rest_url=arguments.myria_url, execution_url=arguments.myria_web_url)

    print 'Arguments: %s' % vars(arguments)
    return arguments

if __name__ == '__main__':
    arguments = parse_arguments(sys.argv[1:])

    print 'INFO: Hybrid NFS version assumes a symbolic links exists at /home/scidb/# pointing to /mnt/scidb/###/#/out'

    build_input(arguments)
    ensure_symbols_relation(arguments)

    restart_scidb(arguments)
    warm_array(arguments)

    scidb_time = time_scidb(scidb_query(arguments), arguments)
    myria_time = time_myria(myria_plan(arguments), arguments)

    print 'SciDB execution time: %0.2f' % scidb_time
    print 'Myria execution time: %0.2f' % myria_time
    print 'Hybrid execution time: %0.2f' % (scidb_time + myria_time)
