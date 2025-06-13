import argparse
import logging

from common.util import b_to_mb
from database.replica import Replica
from distributed.tuner import Tuner
from workload.workload_parser import WorkloadParser

def get_replicas(path = './replicas.csv') -> list[Replica]:
    replicas = []
    with open(path, 'r') as infile:
        lines = infile.readlines()
        for config in lines:
            fields = config.split(',')
            replicas.append(
                Replica(
                    id=fields[0],
                    hostname=fields[1],
                    port=fields[2],
                    dbname=fields[3],
                    user=fields[4],
                    password=fields[5]
                )
            )
    return replicas

def get_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument('-t', '--tuning-parameter', type=float, default=0.5, help='the tuning parameter for the workload-aware routing algorithm')
    parser.add_argument('-b', '--space-budget', type=int, default=6e9, help='the space budget for each database replica')
    parser.add_argument('-w', '--max-index-width', type=int, default=2, help='the maximum width of an index recommended by the Extend algorithm')
    parser.add_argument('-r', '--replicas', type=str, default='./replicas.csv', help='the path to the replicas csv file')
    parser.add_argument('-q', '--queries', type=str, default='./queries.txt', help='the path to the text file containing the query workload')
    parser.add_argument('-v', '--verbose', action='store_true', help='enable more debug logging output')

    return parser.parse_args()

if __name__ == '__main__':
    args = get_arguments()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)

    replicas = get_replicas(args.replicas)
    parser = WorkloadParser('postgres', replicas[0].dbname, 'tpch', replicas[0].connection_string())
    workload = parser.execute()
    for query in workload.queries:
        query.text = replicas[0].conn.update_query_text(query.text)
    tuner = Tuner(workload.queries, replicas, b_to_mb(args.space_budget), args.max_index_width)

    config, routes = tuner.run(args.tuning_parameter)

    print('=' * 20)
    print('INDEX CONFIGURATION\n')
    print(config)
    print('\nROUTING TABLE')
    print(','.join(routes))
    print('=' * 20)
