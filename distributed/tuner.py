import numpy as np
from scipy.cluster.hierarchy import fclusterdata
from distributed.query import Query
from database.replica import Replica
from extend.index import Index

class Tuner:
    def __init__(self, queries: list[Query], replicas: list[Replica]):
        self.queries = queries
        self.replicas = replicas
        self.n_queries = len(queries)
        self.n_replicas = len(replicas)

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
        partitions = self.cluster(self.queries, self.n_replicas)
        configs = [self.recommend_configuration(self.replicas[i_rep], partitions[i_rep]) for i_rep in range(self.n_replicas)]
        best_cost = sum([self.compute_replica_cost(self.replicas[i_rep], partitions[i_rep]) for i_rep in range(self.n_replicas)])

        next_configs = []
        next_partitions = []

        while True:
            next_partitions = [[] for _ in range(self.n_replicas)]

            for query in self.queries:
                this_costs = [
                    replica.conn.get_cost(query.text)
                    for replica in self.replicas
                ]
                best_fit = np.argmin(this_costs)
                next_partitions[best_fit].append(query)
            
            next_configs = [self.recommend_configuration(self.replicas[i_rep], next_partitions[i_rep]) for i_rep in range(self.n_replicas)]
            next_cost = sum([self.compute_replica_cost(self.replicas[i_rep], next_partitions[i_rep]) for i_rep in range(self.n_replicas)])
            partitions = next_partitions

            if next_cost < best_cost:
                configs = next_configs
                best_cost = next_cost
            else:
                break
        
        return configs, partitions
            

    def compute_replica_cost(self, replica: Replica, workload: list[Query]) -> float:
        cost = 0

        for query in workload:
            cost += replica.conn.get_cost(query)

        return cost

    def cluster(self, queries: list[Query], n_clusters: int) -> list[list[Query]]:
        def metric(x, y):
            # something of a kludge. we need a custom (jaccard) metric
            # function anyway, but we're actually passing indices to scipy
            return queries[x].similarity(queries[y])
        
        X = [i for i in range(self.n_queries)]
        assignments = fclusterdata(X, n_clusters, criterion='maxclust', metric=metric)
        clusters = [[] for _ in range(n_clusters)]

        for query, assignment in enumerate(assignments):
            clusters[assignment].append(queries[query])

        return clusters

    def recommend_configuration(self, replica: Replica, workload: list[Query]):
        # we need to recreate this every time, unfortunately
        replica.create_extend_algorithm()
        
        return replica.algorithm.calculate_best_indexes(workload)