import sys
import subprocess
import math
import time
import timeit
import argparse
import scidbpy

def time_execution(arguments, n=1):
    return timeit.Timer(lambda: execute(arguments)).timeit(number=n)

def execute(arguments):
    try:
        subprocess.check_output([arguments.scidb_iquery, '-anq', query(arguments)], stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        print e.cmd
        print e.output
        raise e


def query(arguments):
    materialization, query = knn(arguments, *condition(arguments, *idf(arguments, *symbols(arguments))))
    return '''
        {}
        {}
        consume({});
        '''.format(
                transform_arrays(arguments),
                materialization,
                query)

def transform_arrays(arguments):
    return '\n'.join(['''create temp array {name}
                           <value: double null, bucket:int64 null>
                           [id=0:{patients},{chunk_patients},{overlap_patients},
                            time=0:{vector_size},{chunk_vectors},{overlap_vectors}];'''.format(
                                name=get_transform_name(arguments, i+1),
                                patients=arguments.patients-1,
                                chunk_patients=arguments.chunk_patients,
                                overlap_patients=arguments.overlap_patients,
                                vector_size=arguments.vector_size-1,
                                chunk_vectors=arguments.chunk_vectors,
                                overlap_vectors=arguments.overlap_vectors)
                            for i in xrange(arguments.iterations)])

def knn(arguments, materialization, conditioned_tfv):
    name = get_name(arguments, 'conditioned_tfv')
    return (materialization +
        'store({conditioned_tfv}, {name});'.format(conditioned_tfv=conditioned_tfv, name=name),
        '''
            project(
                apply(
                    aggregate(
                        apply(
                            cross_join(
                                attribute_rename(
                                    cross_join(
                                        {conditioned_tfv} as ctfv,
                                        aggregate(
                                            apply(
                                                {conditioned_tfv},
                                                vv,
                                                pow(value, 2)),
                                            sum(vv) as ss,
                                            id) as sum_squares,
                                        ctfv.id,
                                        sum_squares.id),
                                    value,
                                    value) as ctfv_ss,
                                attribute_rename(
                                    redimension(
                                        slice(
                                            {conditioned_tfv},
                                            id,
                                            {test_id}),
                                        <value: double null>[bucket]),
                                    value,
                                    value_test) as test_vector,
                                ctfv_ss.bucket,
                                test_vector.bucket),
                            product,
                            value * value_test),
                        sum(product) as sum,
                        min(ss) as ss,
                        id),
                    rho,
                    sum / ss),
                rho)
            '''.format(conditioned_tfv=name,
                       test_id=arguments.test_id))

def condition(arguments, materialization, tfv):
    name = get_name(arguments, 'tfv')
    return (materialization +
        'store({tfv}, {name});'.format(tfv=tfv, name=name),
        '''
            attribute_rename(
                project(
                    apply(
                        cross_join(
                            {name} as _tfv,
                            aggregate(
                                {name},
                                avg(idf) as mean,
                                stdev(idf) as std,
                                id) as moments,
                            _tfv.id,
                            moments.id),
                        normalized_value,
                        (idf - mean) / std),
                    normalized_value),
                normalized_value,
                value)
    '''.format(name=name))

def join_tfv_moments(frequencies, value_indices, unique_values):
    return '''
        cross_join(
            redimension(
                project(
                    index_lookup(
                        {frequencies},
                        {unique_values},
                        {frequencies}.value,
                        value_index),
                    value,
                    frequency,
                    value_index),
                <value:int64 NULL DEFAULT null> [id,bucket,value_index]) as left,
            attribute_rename(
                redimension(
                    project(
                        index_lookup(
                            {frequencies},
                            {unique_values},
                            {frequencies}.value,
                            value_index),
                        value,
                        frequency,
                        value_index),
                    <value:int64 NULL, frequency:uint64 NULL> [index, value_index]),
                value,
                value_f) as right,
            left.value_index,
            right.value_index)
        '''.format(
            frequencies=frequencies,
            value_indices=value_indices,
            unique_values=unique_values)

def idf(arguments, materialization, symbols):
    symbols_name = get_name(arguments, 'symbols')
    unique_values_name = get_name(arguments, 'unique_values')
    value_indices_name = get_name(arguments, 'value_indices')
    frequencies_name = get_name(arguments, 'frequencies')
    materialization += '''
         store({symbols}, {symbols_name});
         store({unique_values}, {unique_values_name});
         store({value_indices}, {value_indices_name});
         store({frequencies}, {frequencies_name});'''.format(
            symbols=symbols,
            symbols_name=symbols_name,
            unique_values_name=unique_values_name,
            unique_values=unique_values(symbols_name),
            value_indices=value_indices(symbols_name, unique_values_name),
            value_indices_name=value_indices_name,
            frequencies=create_frequencies(symbols_name, value_indices_name),
            frequencies_name=frequencies_name)
    return (materialization,
            '''
              redimension(
                apply(
                  {joined_frequencies},
                  idf,
                  log({patients} / frequency) * value),
                <idf:double null>[id, bucket])'''.format(
                    patients=float(arguments.patients),
                    joined_frequencies=join_symbols_frequencies(
                        frequencies_name,
                        value_indices_name,
                        unique_values_name)))

    return '''store({symbols}, {name});
              store({unique_values}, {unique_values_name});
              store({value_indices}, {value_indices_name});
              store({frequencies}, {frequencies_name});
              redimension(
                apply(
                  {joined_frequencies},
                  idf,
                  log({patients} / frequency) * value),
                <idf:double null>[id, bucket])'''.format(
                    patients=float(arguments.patients),
                    symbols=symbols,
                    name=symbols_name,
                    unique_values_name=unique_values_name,
                    unique_values=unique_values(symbols_name),
                    value_indices=value_indices(symbols_name, unique_values_name),
                    value_indices_name=value_indices_name,
                    frequencies=create_frequencies(symbols_name, value_indices_name),
                    frequencies_name=frequencies_name,
                    joined_frequencies=join_symbols_frequencies(frequencies_name, value_indices_name, unique_values_name))

def join_symbols_frequencies(frequencies, value_indices, unique_values):
    return '''
        cross_join(
            redimension(
                {value_indices},
                <value:int64 NULL DEFAULT null> [id,bucket,value_index]) as left,
            attribute_rename(
                redimension(
                    project(
                        index_lookup(
                            {frequencies},
                            {unique_values},
                            {frequencies}.value,
                            value_index),
                        value,
                        frequency,
                        value_index),
                    <value:int64 NULL, frequency:uint64 NULL> [index, value_index]),
                value,
                value_f) as right,
            left.value_index,
            right.value_index)
        '''.format(
            frequencies=frequencies,
            value_indices=value_indices,
            unique_values=unique_values)

def create_frequencies(array_name, value_index_name):
    return '''
        project(
            unpack(
                aggregate(
                    redimension(
                        {value_indices},
                        <value:int64 null>[id,time,value_index]),
                    count(*) as frequency,
                    max(value) as value,
                    value_index),
                index),
            value, frequency)'''.format(array=array_name,
                                        value_indices=value_index_name)

def unique_values(symbols):
    return 'uniq(sort(project({symbols}, value)))'.format(symbols=symbols)

def value_indices(symbols, unique_values):
    return '''
        project(
            index_lookup(
                {symbols},
                {unique_values},
                {symbols}.value,
                value_index),
            value,
            value_index)
        '''.format(symbols=symbols, unique_values=unique_values)

def symbols(arguments):
    materialization = '\n'.join([bin_vector(arguments, i) for i in xrange(arguments.iterations)])
    return (materialization,
            '''attribute_rename(
                 project(
                   apply({}, signed_value, int64(value)),
                   signed_value),
               signed_value, value)'''.format(reduce(lambda aggregate, index: concat_histograms(arguments, aggregate, index+1),
                                            xrange(1, arguments.iterations),
                                            create_histogram(arguments, get_transform_name(arguments, 1)))))

def concat_histograms(arguments, current, index):
    return '''concat({},
                     {})'''.format(
                  current,
                  create_histogram(arguments, get_transform_name(arguments, index)))

def create_histogram(arguments, source):
    return ('redimension({source}, <value:uint64 null>['
              'id=0:{patients},{chunk_patients},{overlap_patients}, '
              'bucket=0:{bins},{chunk_bins},{overlap_bins}], true, count(bucket) as value)').format(
             source=source,
             patients=arguments.patients-1,
             chunk_patients=arguments.chunk_patients,
             overlap_patients=arguments.overlap_patients,
             bins=arguments.bins-1,
             chunk_bins=arguments.chunk_bins,
             overlap_bins=arguments.overlap_bins)

def bin_vector(arguments, index):
    return 'store(regrid(scan({input}), 1, 2, avg(value) as value, bin{range}(value) as bucket), {output});'.format(
              range=2**index,
              input=get_transform_name(arguments, index),
              output=get_transform_name(arguments, index+1))

def get_transform_name(arguments, index):
    return get_name(arguments, arguments.transform_format % index) if index else arguments.input_array


def get_name(arguments, name):
    return name + '_' + arguments.suffix


def build_input(arguments):
    print 'Creating input vectors (mode=%s)' % arguments.input_mode
    if arguments.input_mode.lower() == 'deterministic':
        arguments.input_array = (
            scidbpy.connect(arguments.url, username=arguments.username, password=arguments.password)
                   .afl.build('<value: double>[id=0:{},{},{}, time=0:{},{},{}]'.format(
                                  arguments.patients-1, arguments.chunk_patients, arguments.overlap_patients,
                                  arguments.vector_size-1, arguments.chunk_vectors, arguments.overlap_vectors),
                              '(double(id+1) / (time+1)) / %f - 1' % (arguments.patients/2))
                   .eval()
                   .name)
    else:
        arguments.input_array = (
            scidbpy.connect(arguments.url, username=arguments.username, password=arguments.password)
                   .random((arguments.patients, arguments.vector_size),
                           chunk_size=(arguments.chunk_patients, arguments.chunk_vectors),
                           dim_names=['id', 'time'])
                   .attribute_rename('f0', 'value')
                   .eval()
                   .name)


def restart_scidb(arguments):
    print 'Restarting SciDB'

    if not arguments.restart:
        return
    #if arguments.fast:
    #  return

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
        subprocess.check_output([arguments.scidb_iquery, '-anq', 'scan({})'.format(arguments.input_array)], stderr=subprocess.STDOUT)
        subprocess.check_output([arguments.scidb_iquery, '-anq', "load_library('bin')"], stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        print e.cmd
        print e.output
        raise e


def parse_arguments(arguments):
    parser = argparse.ArgumentParser(description='Execute SciDB-only test')
    parser.add_argument('patients', type=int, help='Number of patients to evaluate')
    parser.add_argument('vector_size', type=int, help='Size of input vectors for each patient')

    parser.add_argument('--bins', type=int, default=10, help='Number of histogram bins')
    parser.add_argument('--url', type=str, default='http://localhost:8080', help='SciDB Shim URL')

    parser.add_argument('--test-id', dest='test_id', type=int, default=1, help='Index of test patient for k-NN computation')
    parser.add_argument('--input-mode', dest='input_mode', type=str, default='random', choices=['deterministic', 'random'], help='Mode of automatically generated input')

    parser.add_argument('--scidb-bin', dest='scidb_bin', type=str, default='/opt/scidb/14.12/bin/scidb.py', help='Path of scidb.py')
    parser.add_argument('--scidb-iquery', dest='scidb_iquery', type=str, default='/opt/scidb/14.12/bin/iquery', help='Path of iquery')
    parser.add_argument('--scidb-name', dest='scidb_name', type=str, default='mydb', help='Name of SciDB database')

    parser.add_argument('--chunk-patients', dest='chunk_patients', type=int, default=1, help='Chunk size for patient array')
    parser.add_argument('--chunk-vectors', dest='chunk_vectors', type=int, default=None, help='Chunk size for input vectors')
    parser.add_argument('--chunk-bins', dest='chunk_bins', type=int, default=2**32, help='Chunk size for histogram bins')

    parser.add_argument('--overlap-vectors', dest='overlap_vectors', type=int, default=0, help='Array overlap for input vectors')
    parser.add_argument('--overlap-patients', dest='overlap_patients', type=int, default=0, help='Array overlap for patient array')
    parser.add_argument('--overlap-bins', dest='overlap_bins', type=int, default=0, help='Array overlap for histogram bins')

    parser.add_argument('--username', type=str, default=None, help='Username used to authenticate with SciDB Shim')
    parser.add_argument('--password', type=str, default=None, help='Password used to authenticate with SciDB Shim')

    parser.add_argument('--fast', action='store_true', help='Favor speed over fair benchmark results')
    parser.add_argument('--restart', dest='restart', action='store_true', help='Restart SciDB before testing')
    parser.add_argument('--no-restart', dest='restart', action='store_false', help='Do not restart SciDB before testing')
    parser.set_defaults(restart=True)

    arguments = parser.parse_args(arguments)
    arguments.chunk_vectors = arguments.chunk_vectors or arguments.vector_size
    arguments.transform_format = 'transform_%d'
    arguments.suffix = str(int(time.time()))
    arguments.iterations = int(math.log(arguments.vector_size, 2))

    print 'Arguments: %s' % vars(arguments)
    return arguments

if __name__ == "__main__":
    arguments = parse_arguments(sys.argv[1:])
    build_input(arguments)
    restart_scidb(arguments)
    warm_array(arguments)
    print 'SciDB execution time: %0.2f' % time_execution(arguments)