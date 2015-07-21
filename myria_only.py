import sys
import os.path
import subprocess
import time
import random
import tempfile
import argparse
from myria import *

QUERY = """
const test_vector_id: {id};

vectors = scan({relation});

-------------------------
-- Constants + Functions
-------------------------
const alpha: 1.0;
const bins: {bins};

def log2(x): log(x) / log(2);
def mod2(x): x - int(x/2)*2;
def iif(expression, true_value, false_value):
    case when expression then true_value
         else false_value end;
def bucket(x, high, low): greater(least(int((bins-1) * (x - low) / iif(high != low, high - low, 1)),
                                bins - 1), 0);
def difference(current, previous, previous_time, time):
    iif(previous_time >= 0,
        (current - previous) * iif(previous_time < time, 1, -1),
        current);
def idf(w_ij, w_ijN, N): log(N / w_ijN) * w_ij;

symbols = empty(id:int, index:int, value:int);

------------------------------------------------------------------------------------
-- Harr Transform
------------------------------------------------------------------------------------
uda HarrTransformGroupBy(alpha, time, x) {{
  [0.0 as coefficient, 0.0 as _sum, 0 as _count, -1 as _time];
  [difference(x, coefficient, _time, time), _sum + x, _count + 1, time];
  [coefficient, _sum / int(_count * alpha)];
}};

iterations = [from vectors where id = test_vector_id emit 0 as i, int(ceil(log2(count(*)))) as total];

do
    groups = [from vectors emit
                     id,
                     int(floor(time/2)) as time,
                     HarrTransformGroupBy(alpha, time, value) as [coefficient, mean]];

    coefficients = [from groups emit id, coefficient];
    range = [from vectors emit max(value) - min(value) as high, min(value) - max(value) as low];

    histogram = [from coefficients, range
                 emit id,
                      bucket(coefficient, high, low) as index,
                      count(bucket(coefficient, high, low)) as value];
    symbols = symbols + [from histogram, iterations emit id, index + i*bins as index, value];
    vectors = [from groups emit id, time, mean as value];

    iterations = [from iterations emit $0 + 1, $1];
while [from iterations emit $0 < $1];

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

def restart_myria(arguments):
    if arguments.fast: return

    print 'Restarting Myria'
    os.chdir(arguments.install_path + '/myriadeploy')
    try:
        subprocess.check_output(['./kill_all_java_processes.py', arguments.deployment_path], stderr=subprocess.STDOUT)
        subprocess.check_output(['./launch_cluster.sh', arguments.deployment_path], stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        print e.cmd
        print e.output
        raise e
    time.sleep(4)

def create_input_files(arguments):
    print 'Creating input files on coordinator'
    connection = MyriaConnection(rest_url=arguments.url, execution_url=arguments.execution_url)
    workers = len(connection.workers())
    files = []
    offset = 0

    if arguments.fast and MyriaRelation("public:adhoc:" + arguments.name).is_persisted:
      print '  Relation already exists; skipping'
      return

    for w in xrange(workers):
        filename = (arguments.tmp_path or '/tmp') + '/ingest-%dx%d-%s-%dof%d.csv' % (
            arguments.patients, arguments.vector_size, arguments.input_mode, w+1, workers)
        files.append(filename)
        print '  Input %d: %s' % (w, filename)

        if not os.path.isfile(filename):
            with open(filename, 'w') as file:
                for i in xrange(arguments.patients/workers):
                    for j in xrange(arguments.vector_size):
                        if arguments.input_mode == 'deterministic':
                            file.write('%d,%d,%f\n' % (i + offset, j, float(i + offset + 1) / (j+1) / (arguments.patients/2) - 1))
                        else:
                            file.write('%d,%d,%f\n' % (i + offset, j, random.uniform(-1, 1)))

                offset += int(arguments.patients/workers)

    return files

def copy_files(files, arguments):
    print 'Copying files to workers'
    connection = MyriaConnection(rest_url=arguments.url, execution_url=arguments.execution_url)

    if arguments.fast and MyriaRelation("public:adhoc:" + arguments.name).is_persisted:
      print '  Relation already exists; skipping'
      return

    try:
        for i, worker in enumerate(connection.workers()):
            node = connection._session.get(connection._url_start + '/workers/worker-{}'.format(worker)).text.split(':')[0]
            subprocess.check_output(['scp', files[i], '{}:{}'.format(node, files[i])], stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        print e.cmd
        print e.output
        raise e

def ingest(files, arguments):
    connection = MyriaConnection(rest_url=arguments.url, execution_url=arguments.execution_url)

    if arguments.fast and MyriaRelation("public:adhoc:" + arguments.name).is_persisted:
      print '  Relation already exists; skipping'
      return

    work = [(w+1, 'file://' + files[w]) for w in xrange(len(connection.workers()))]
    schema = MyriaSchema(
        {"columnNames": ['id','time','value'],
         "columnTypes": ["LONG_TYPE", "LONG_TYPE", "DOUBLE_TYPE"] })
    destination = MyriaRelation("public:adhoc:" + arguments.name,
                                schema=schema,
                                connection=connection)

    query = MyriaQuery.parallel_import(destination, work)
    try:
      query.wait_for_completion(timeout=3600)
    except AttributeError as e:
      if query.status != 'SUCCESS': raise e
    print 'Ingesting into public:adhoc:%s (%s)' % (arguments.name, query.status)

def partition(arguments):
    connection = MyriaConnection(rest_url=arguments.url, execution_url=arguments.execution_url)

    if arguments.fast and MyriaRelation("public:adhoc:" + arguments.name).is_persisted:
      print '  Relation already exists; skipping'
      return

    query = MyriaQuery.submit("""x = scan({name});
                                 store(x, {name}, [id]);""".format(name=arguments.name),
                              connection=connection)
    try:
      query.wait_for_completion(timeout=3600)
    except AttributeError as e:
      if query.status != 'SUCCESS': raise e
    print 'Partitioning input by id (%s)' % query.status

def warm_relation(arguments):
    if arguments.fast:
      return

    connection = MyriaConnection(rest_url=arguments.url, execution_url=arguments.execution_url)
    query = MyriaQuery.submit("""x = scan({name});
                                 store(x, temp);""".format(name=arguments.name),
                              connection=connection)
    try:
      query.wait_for_completion(timeout=3600)
    except AttributeError as e:
      if query.status != 'SUCCESS': raise e
    print 'Warmed input relation (%s)' % arguments.name

def ensure_symbols_relation(arguments):
    connection = MyriaConnection(rest_url=arguments.url, execution_url=arguments.execution_url)
    query = MyriaQuery.submit("""
      singleton_symbols = empty(id:int, index:int, value:int);
      shuffled_symbols = [from singleton_symbols emit id, index, value, count(*)];
      symbols = [from shuffled_symbols emit id, index, value];
      store(symbols, symbols);
      """, connection=connection)
    try:
      query.wait_for_completion(timeout=3600)
    except AttributeError as e:
      if query.status != 'SUCCESS': raise e
    return arguments.myria_connection.get_query_status(query.query_id)['elapsedNanos'] / 1E9 \
                     if query.status == 'SUCCESS' else None

def execute(query, arguments):
    connection = MyriaConnection(rest_url=arguments.url, execution_url=arguments.execution_url)
    query = MyriaQuery.submit(
        query.format(id=arguments.id, relation=arguments.name, bins=arguments.bins),
        connection=connection)
    try:
        query.wait_for_completion(timeout=3600)
    except AttributeError:
        # Workaround when no relations are returned by query
        pass

    total_time = connection.get_query_status(query.query_id)['elapsedNanos'] / 1E9 \
                     if query.status == 'SUCCESS' else None
    print 'Myria query execution time: %0.2f' % total_time
    return total_time

def parse_arguments(arguments):
    parser = argparse.ArgumentParser(description='Execute Myria-only test')
    parser.add_argument('patients', type=int, default=600, help='Number of patients in test')
    parser.add_argument('vector_size', type=int, default=256, help='Size of input vector')
    parser.add_argument('--bins', type=int, default=10, help='Number of histogram bins')

    parser.add_argument('--url', type=str, default='http://localhost:8753', help='Myria REST URL')
    parser.add_argument('--execution-url', dest='execution_url', type=str, help='Myria Execution URL')

    parser.add_argument('--test-id', dest='id', type=int, default=1, help='Index of the test vector')
    parser.add_argument('--input-mode', dest='input_mode', type=str, default='random', choices=['deterministic', 'random'], help='Mode of automatically generated input')

    parser.add_argument('--install-path', dest='install_path', type=str, default='~/myria', help='Myria installation path')
    parser.add_argument('--deployment-file', dest='deployment_path', type=str, default='~/deployment.cfg.ec2', help='Myria deployment file')
    parser.add_argument('--relation-name', dest='name', type=str, default='relation{}x{}', help='Name of input relation')
    parser.add_argument('--tmp-path', dest='tmp_path', type=str, default=tempfile.gettempdir(), help='Temporary directory to store input relation partitions')

    parser.add_argument('--fast', action='store_true', help='Favor speed over fair benchmark results')

    arguments = parser.parse_args(arguments)
    arguments.name = arguments.name.format(arguments.patients, arguments.vector_size)
    arguments.install_path = os.path.expanduser(arguments.install_path)
    arguments.deployment_path = os.path.expanduser(arguments.deployment_path)
    arguments.tmp_path = os.path.expanduser(arguments.tmp_path)
    print arguments
    return arguments

if __name__ == "__main__":
    arguments = parse_arguments(sys.argv[1:])

    restart_myria(arguments)
    ensure_symbols_relation(arguments)
    files = create_input_files(arguments)
    copy_files(files, arguments)
    ingest(files, arguments)
    partition(arguments)
    warm_relation(arguments)
    execute(QUERY, arguments)
