import sys
import subprocess
import math
import timeit
import argparse
import scidbpy

def time_execution(arguments, n=1):
    return timeit.Timer(lambda: execute(arguments)).timeit(number=n)

def execute(arguments):
  sym = symbols(arguments)
  tfv = idf(sym, arguments)
  ctfv = condition(tfv, arguments)
  return knn(ctfv, arguments)

def symbols(arguments):
  vectors, symbols = arguments.input, None

  for index in xrange(arguments.iterations):
      transform = arguments.sdb.afl.regrid(vectors, '1, 2', 'avg(value) as value, '
                                                    'bin{range}(value) as bucket'.format(range=2**index)).eval()
      #transform = (arguments.sdb.afl.regrid(vectors, '1, 2', 'avg(value) as value, '
      #                                              'avg(value) as bucket'.format(range=2**index))
      #                              .apply('signed_bucket', 'int64(bucket)')
      #                              .project('value', 'signed_bucket')
      #                              .attribute_rename('signed_bucket', 'bucket')
      #                              .eval())

      histogram = (transform.redimension('<value:uint64 null>[bucket=0:{},1000000,0, id=0:{},30,0]'.format(
                                             arguments.bins-1, arguments.patients-1),
                                         'count(bucket) as value')
                            .apply('signed_value', 'int64(value)')
                            .project('signed_value')
                            .attribute_rename('signed_value', 'value'))
      symbols = symbols.concat(histogram) if symbols else histogram
      vectors = transform

  final = symbols.redimension('<value:int64 null>[id=0:{},30,0, bucket=0:{},1000000,0]'.format(
                                  arguments.patients-1, arguments.bins*arguments.iterations-1))

  return final.eval()

def idf(symbols, arguments):
    frequencies = (symbols.groupby('value')
                          .aggregate('count(*) as frequency')
                          .project('value', 'frequency'))

    tfv = (symbols.interface.merge(
                     symbols.redimension('<value:int64 null>[id, bucket]'),
                     frequencies,
                     on='value', suffixes=('', '_f'))
                  .apply('idf', 'log({} / frequency) * value'.format(float(arguments.patients)))
                  .redimension('<idf:double null>[id=0:{},30,0, bucket=0:{},1000000,0]'.format(
                     arguments.patients-1, arguments.bins*arguments.iterations-1)))

    return tfv.eval()


def condition(tfv, arguments):
    moments = tfv.aggregate('avg(idf) as mean', 'stdev(idf) as std', 'id')

    conditioned_tfv = (tfv.interface.merge(tfv, moments, on='id')
                                    .apply('normalized_value', '(idf - mean) / std')
                                    .project('normalized_value')
                                    .attribute_rename('normalized_value', 'value'))

    return conditioned_tfv.eval()


def knn(conditioned_tfv, arguments):
    test_vector = (conditioned_tfv.isel(id=arguments.test_id)
                                  .redimension('<value: double null>[bucket=0:{},1000000,0]'.format(
                                    arguments.bins*arguments.iterations - 1)))

    sum_squares = conditioned_tfv.interface.merge(
                                    conditioned_tfv,
                                    conditioned_tfv.apply('vv', 'pow(value, 2)')
                                                   .aggregate('sum(vv) as ss', 'id'),
                                    on='id')

    correlations = (conditioned_tfv.interface.merge(sum_squares, test_vector,
                                                    on='bucket',
                                                    suffixes=('', '_test'))
                                   .apply('product', 'value * value_test')
                                   .aggregate('sum(product) as sum', 'min(ss) as ss', 'id')
                                   .apply('rho', 'sum / ss')
                                   .project('rho'))

    return correlations.eval()


def create_input(arguments):
    print 'Creating vectors (mode=%s)' % arguments.input_mode
    if arguments.input_mode.lower() == 'deterministic':
      return (arguments
                .sdb.afl
                .build('<value: double>[id=0:{},{},1, time=0:{},{},1]'.format(
                         arguments.patients-1, arguments.chunk_patients,
                         arguments.vector_size-1, arguments.chunk_vectors),
                       '(double(id+1) / (time+1)) / %f - 1' % (arguments.patients/2))
                .eval())
    else:
      return (arguments
                .sdb
                .random((arguments.patients, arguments.vector_size),
                        chunk_size=(arguments.chunk_patients, arguments.chunk_vectors),
                        dim_names=['id', 'time'])
                .attribute_rename('f0', 'value')
                .eval())


def restart_scidb(arguments):
    if not arguments.restart:
      return

    print 'Restarting SciDB'
    try:
        subprocess.check_output([arguments.scidb_bin, 'stopall', arguments.scidb_name], stderr=subprocess.STDOUT)
        subprocess.check_output([arguments.scidb_bin, 'startall', arguments.scidb_name], stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        print e.cmd
        print e.output
        raise e


def warm_array(arguments):
    print 'Prescanned array'
    try:
        subprocess.check_output(['iquery', '-anq', 'scan({})'.format(arguments.input.name)], stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        print e.cmd
        print e.output
        raise e


def parse_arguments(arguments):
    parser = argparse.ArgumentParser(description='Execute Myria-only test')
    parser.add_argument('patients', type=int, help='Number of patients to evaluate')
    parser.add_argument('vector_size', type=int, help='Size of input vectors for each patient')

    parser.add_argument('--url', type=str, default='http://localhost:8080', help='SciDB Shim URL')
    parser.add_argument('--bins', type=int, default=10, help='Number of histogram bins')

    parser.add_argument('--test-id', dest='test_id', type=int, default=1, help='Index of test patient for k-NN computation')
    parser.add_argument('--input-mode', dest='input_mode', type=str, default='random', choices=['determinstic', 'random'], help='Mode of automatically generated input')

    parser.add_argument('--scidb-bin', dest='scidb_bin', type=str, default='/opt/scidb/14.12/bin/scidb.py', help='Path of scidb.py')
    parser.add_argument('--scidb-name', dest='scidb_name', type=str, default='mydb', help='Name of SciDB database')

    parser.add_argument('--chunk-patients', dest='chunk_patients', type=int, default=1, help='Chunk size for patient array')
    parser.add_argument('--chunk-vectors', dest='chunk_vectors', type=int, default=None, help='Chunk size for input vectors')
    parser.add_argument('--chunk-bins', dest='chunk_bins', type=int, default=2**32, help='Chunk size for histogram bins')

    parser.add_argument('--overlap-vectors', dest='overlap_vectors', type=int, default=0, help='Array overlap for input vectors')
    parser.add_argument('--overlap-patients', dest='overlap_patients', type=int, default=0, help='Array overlap for patient array')
    parser.add_argument('--overlap-bins', dest='overlap_bins', type=int, default=0, help='Array overlap for histogram bins')

    parser.add_argument('--username', type=str, default=None, help='Username used to authenticate with SciDB Shim')
    parser.add_argument('--password', type=str, default=None, help='Password used to authenticate with SciDB Shim')

    parser.add_argument('--restart', dest='restart', action='store_true', help='Restart SciDB before testing')
    parser.add_argument('--no-restart', dest='restart', action='store_false', help='Do not restart SciDB before testing')
    parser.set_defaults(restart=True)

    arguments = parser.parse_args(arguments)
    arguments.chunk_vectors = arguments.chunk_vectors or arguments.vector_size
    arguments.iterations = int(math.log(arguments.vector_size, 2))
    arguments.sdb = scidbpy.connect(arguments.url, username=arguments.username, password=arguments.password)

    print 'Arguments: %s' % vars(arguments)
    return arguments


if __name__ == '__main__':
    arguments = parse_arguments(sys.argv[1:])
    arguments.input = create_input(arguments)
    restart_scidb(arguments)
    warm_array(arguments)

    print 'SciDB-Py execution time: %0.2f' % time_execution(arguments)