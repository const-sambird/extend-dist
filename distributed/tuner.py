import logging
import numpy as np
from copy import deepcopy
from sklearn.cluster import AgglomerativeClustering
from workload.workload import Query, Workload
from database.replica import Replica
from extend.index import Index

class Tuner:
    def __init__(self, queries: list[Query], replicas: list[Replica], budget: int, max_index_width: int):
        self.queries = queries
        self.replicas = replicas
        self.n_queries = len(queries)
        self.n_replicas = len(replicas)
        
        self.extend_configuration = {
            'budget_MB': budget,
            'max_index_width': max_index_width
        }
    
    def run(self, threshold: float) -> tuple[list[list[Index]], list[int]]:
        '''
        Run Hang's algorithm (using Extend).

        Returns an index configuration and routing table.

        This is the public entry point to this algorithm.

        :returns configs: the index configurations
        :returns routes: the routing table
        '''
        config, partitions = self.cluster_and_tune()
        config, partitions = self.balance_tuning_refine(partitions, config)
        routes = self.load_aware_routing(self.queries, config, threshold)

        return config, routes

    def cluster_and_tune(self) -> tuple[list[Index], list[int]]:
        '''
        Algorithm 1 in Hang 2024: `Index Utilization-based Clustering & Tuning'

        Given an *index tuning functionality f* (in this case, Extend), number of
        database replicas *n*, input workload W = {q_1, ..., q_m}, and similarity
        metric S, partition the workload amongst the replicas and generate index
        configurations for each of them.

        :returns config: the list of index configurations for each replica
        :returns routes: the initial routing table generated
        '''
        logging.info('Starting cluster-and-tune (step 1)')
        partitions = self.cluster(self.queries, self.n_replicas)
        configs = [self.recommend_configuration(self.replicas[i_rep], partitions[i_rep]) for i_rep in range(self.n_replicas)]
        best_cost = sum([self.compute_replica_cost(self.replicas[i_rep], partitions[i_rep]) for i_rep in range(self.n_replicas)])

        next_configs = []
        next_partitions = []

        while True:
            next_partitions = self.best_fit_partition()
            
            next_configs = [self.recommend_configuration(self.replicas[i_rep], next_partitions[i_rep]) for i_rep in range(self.n_replicas)]
            next_cost = sum([self.compute_replica_cost(self.replicas[i_rep], next_partitions[i_rep]) for i_rep in range(self.n_replicas)])
            partitions = next_partitions

            logging.debug(f'the next cost is {next_cost}, and the current best cost is {best_cost}')

            if next_cost < best_cost:
                configs = next_configs
                best_cost = next_cost
            else:
                break
        
        logging.debug('derived the following partitions:')
        logging.debug(partitions)
        
        return configs, partitions
            
    def balance_tuning_refine(self, partitions: list[Query], configs: list[Index]):
        '''
        Algorithm 2 in Hang 2024: `Balance-Aware Tuning Refinement'

        Refines the workload partitions and index configurations to attempt
        to mitigate workload skew

        :param partitions: the initial set of workload partitions (from algorithm 1)
        :param configs: a list of index configurations for each replica (from algorithm 1)
        :returns configs: the refined index configurations
        :returns partitions: the refined partitions
        '''
        logging.info('Starting balance-aware tuning refinement (step 2)')
        baseline = self.get_baseline_costs()

        logging.debug('the baseline query costs are:')
        logging.debug(baseline)

        for idx, replica in enumerate(self.replicas):
            replica.set_index_configuration(configs[idx])
        
        curr_partitions = self.best_fit_partition()
        curr_configs = configs
        curr_cost = self.compute_total_cost(self.replicas, curr_partitions)

        while True:
            for idx, replica in enumerate(self.replicas):
                replica.set_index_configuration(curr_configs[idx])
            best_fit_partitions = self.best_fit_partition()
            replica_costs = [
                self.compute_replica_cost(self.replicas[i], curr_partitions[i])
                for i in range(self.n_replicas)
            ]
            worst_replica = np.argmax(replica_costs)
            candidates = list(curr_partitions[worst_replica])
            candidates = [query for query in candidates if query in best_fit_partitions[worst_replica]] # compute intersection
            workload_costs = self.compute_costs_by_query(self.replicas[worst_replica], candidates)
            worst_query = candidates[np.argmax(workload_costs)]
            logging.debug(f'the worst replica is {worst_replica}, containing {curr_partitions[worst_replica]}, and the worst query is {worst_query}')

            dest_replicas = []
            query_costs = self.compute_costs_by_replica(worst_query, self.replicas)
            for i in range(self.n_replicas):
                if i == worst_replica: continue
                if query_costs[i] < baseline[i]:
                    dest_replicas.append(i)

            if len(dest_replicas) > 0:
                dest_replica = None
                min_cost = float('inf')

                for candidate in dest_replicas:
                    if query_costs[candidate] < min_cost:
                        dest_replica = candidate
                        min_cost = query_costs[candidate]
            else:
                dest_replica = None
                min_cost = float('inf')

                for candidate in range(self.n_replicas):
                    if candidate == worst_replica: continue
                    if query_costs[candidate] < min_cost:
                        dest_replica = candidate
                        min_cost = query_costs[candidate]
            
            logging.debug(f'moving or duplicating {worst_query} to {dest_replica}')
            
            # consider MOVE and DUPLICATE operations
            move_partitions = [p[:] for p in curr_partitions]
            move_partitions[worst_replica].remove(worst_query)
            move_partitions[dest_replica].append(worst_query)
            move_config = [
                self.recommend_configuration(self.replicas[i_rep], move_partitions[i_rep])
                for i_rep in range(self.n_replicas)
            ]
            move_cost = self.compute_total_cost(self.replicas, move_partitions)

            duplicate_partitions = [p[:] for p in curr_partitions]
            duplicate_partitions[dest_replica].append(worst_query)
            duplicate_config = [
                self.recommend_configuration(self.replicas[i_rep], duplicate_partitions[i_rep])
                for i_rep in range(self.n_replicas)
            ]
            duplicate_cost = self.compute_total_cost(self.replicas, duplicate_partitions)

            logging.debug(f'current cost {curr_cost}, move cost {move_cost}, duplicate cost {duplicate_cost}')

            if move_cost < duplicate_cost:
                new_partitions = move_partitions
                new_config = move_config
                new_cost = move_cost
            else:
                new_partitions = duplicate_partitions
                new_config = duplicate_config
                new_cost = duplicate_cost
            
            if new_cost < curr_cost:
                curr_partitions = new_partitions
                curr_configs = new_config
                curr_cost = new_cost
            else:
                return curr_configs, curr_partitions
    
    def load_aware_routing(self, queries: list[Query], configs: list[list[Index]], threshold: float) -> list[int]:
        '''
        Algorithm 3 in Hang 2024: `Benefit-First Load-Aware Routing Strategy'
        Generates a routing table given the index configurations generated after
        algorithm 2 completes. Note that the partitions given by algorithm 2 are
        no longer necessary (because we will not be changing the index configurations
        any more).

        :param queries: the workload to route
        :param configs: the index configuration for each database replica
        :param threshold: tuning parameter - roughly 'how much load skew can we tolerate?' in a range between 0 and 1
        :returns routes: which database replica to route each query to
        '''
        logging.info('Starting benefit-first load-aware routing (step 3)')

        baseline_costs = self.get_baseline_costs()
        routes = [-1 for _ in range(self.n_queries)]

        for i_rep in range(self.n_replicas):
            self.replicas[i_rep].set_index_configuration(configs[i_rep])
        
        loads = np.zeros((self.n_replicas,))

        for idx, query in enumerate(queries):
            costs = self.compute_costs_by_replica(query, self.replicas)
            route = self._route_one(query, loads, costs, baseline_costs[idx], threshold)
            routes[idx] = route
            loads[route] += costs[route]
        
        return routes
    
    def _route_one(self, query: Query, loads: list[float], costs: list[float], baseline: float, threshold: float) -> int:
        '''
        Okay, so this is *actually* algorithm 3 in Hang 2024. Given a single query,
        return the index of the replica that it should be routed to. Should only
        be called from `load_aware_routing` as this sets the index simulation properly.

        :param query: the query to route to a replica
        :param loads: the cost currently associated with each replica
        :param costs: the cost of executing the given query on each database replica
        :param baseline: the cost of executing the given query where no indexes exist
        :param threshold: tuning parameter - roughly 'how much load skew can we tolerate?' in a range between 0 and 1
        :returns route: the index of the replica to route this query to
        '''
        order = np.argsort(costs)
        route_to = order[0]

        for i_replica in order[1:]:
            if costs[i_replica] < baseline:
                if (loads[i_replica] / loads[order[0]]) < threshold:
                    route_to = i_replica
        
        logging.debug(f'routing {query} to {route_to}')

        return route_to

    def compute_replica_cost(self, replica: Replica, workload: list[Query]) -> float:
        return sum(self.compute_costs_by_query(replica, workload))
    
    def compute_costs_by_query(self, replica: Replica, workload: list[Query]) -> list[float]:
        costs = []

        for query in workload:
            costs.append(replica.conn.get_cost(query))
        
        return costs
    
    def compute_costs_by_replica(self, query: Query, replicas: list[Replica]) -> list[float]:
        costs = []

        for replica in replicas:
            costs.append(replica.conn.get_cost(query))
        
        return costs
    
    def compute_total_cost(self, replicas: list[Replica], partitions: list[list[Query]]) -> float:
        cost = 0

        for i in range(len(replicas)):
            cost += self.compute_replica_cost(replicas[i], partitions[i])

        return cost

    def best_fit_partition(self) -> list[list[Query]]:
        '''
        Get the partition of the queries in the workload under a
        'best fit' strategy; ie, route all queries to the replica
        where they will be executed with the lowest cost.

        :returns: the partitioned workload
        '''
        partitions = [[] for _ in range(self.n_replicas)]

        for query in self.queries:
            this_costs = [
                replica.conn.get_cost(query)
                for replica in self.replicas
            ]
            best_fit = np.argmin(this_costs)
            partitions[best_fit].append(query)
        
        return partitions

    def cluster(self, queries: list[Query], n_clusters: int) -> list[list[Query]]:
        def metric(x, y):
            return queries[x].similarity(queries[y])
        
        n_queries = len(queries)
        distance_matrix = np.zeros((n_queries, n_queries))

        for i in range(n_queries):
            for j in range(n_queries):
                if i == j: continue
                distance_matrix[i][j] = metric(i, j)
        
        assignments = AgglomerativeClustering(n_clusters=n_clusters, metric='precomputed', linkage='complete').fit(distance_matrix).labels_
        clusters = [[] for _ in range(n_clusters)]

        for query, assignment in enumerate(assignments):
            clusters[assignment - 1].append(queries[query])

        return clusters

    def recommend_configuration(self, replica: Replica, workload: list[Query]):
        # we need to recreate this every time, unfortunately
        replica.reset()
        replica.create_extend_algorithm(self.extend_configuration)
        
        config = replica.algorithm.calculate_best_indexes(Workload(workload))
        replica.set_index_configuration(config)

        return config
    
    def get_baseline_costs(self) -> list[float]:
        '''
        Get the predicted query costs of every query in the
        workload without any indexes created.

        **Note: uses replica 0 to run benchmarks, and will reset the current index configuration!**

        :returns: a list of the query costs
        '''
        replica = self.replicas[0]
        replica.reset()

        costs = []

        for query in self.queries:
            costs.append(replica.conn.get_cost(query))
        
        return costs
