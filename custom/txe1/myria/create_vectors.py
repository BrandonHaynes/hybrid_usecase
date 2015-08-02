import sys
sys.path.append('../../..')

from myria_only import *

if __name__ == "__main__":
    arguments = parse_arguments(sys.argv[1:])

    files = create_input_files(arguments)
    copy_files(files, arguments)
    ingest(files, arguments)
    partition(arguments)
    warm_relation(arguments)
