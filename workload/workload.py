from typing import List

from extend.index import Index


class Query:
    def __init__(self, query_id, query_text, columns=None):
        self.nr = query_id
        self.text = query_text

        # Indexable columns
        if columns is None:
            self.columns = []
        else:
            self.columns = columns

    def __repr__(self):
        return f"Q{self.nr}"
    
    def similarity(self, other) -> float:
        '''
        Computes the Jaccard similarity of the two queries
        by the sets of candidate indexes that they generate.

        Subtract from 1 because we use this as a difference function.
        '''
        this_candidates = set(self.columns)
        other_candidates = set(other.columns)

        return 1 - (len(this_candidates & other_candidates) / len(this_candidates | other_candidates))


class Workload:
    queries: List[Query]

    def __init__(self, queries):
        self.queries = queries

    def indexable_columns(self):
        indexable_columns = set()
        for query in self.queries:
            indexable_columns |= set(query.columns)
        return sorted(list(indexable_columns))

    def potential_indexes(self):
        return sorted([Index([c]) for c in self.indexable_columns()])


class Column:
    def __init__(self, name):
        self.name = name.lower()
        self.table = None

    def __lt__(self, other):
        return self.name < other.name

    def __repr__(self):
        return f"C {self.table}.{self.name}"

    # We cannot check self.table == other.table here since Table.__eq__()
    # internally checks Column.__eq__. This would lead to endless recursions.
    def __eq__(self, other):
        if not isinstance(other, Column):
            return False

        assert (
            self.table is not None and other.table is not None
        ), "Table objects should not be None for Column.__eq__()"

        return self.table.name == other.table.name and self.name == other.name

    def __hash__(self):
        return hash((self.name, self.table.name))


class Table:
    def __init__(self, name):
        self.name = name.lower()
        self.columns = []

    def add_column(self, column):
        column.table = self
        self.columns.append(column)

    def add_columns(self, columns):
        for column in columns:
            self.add_column(column)

    def __repr__(self):
        return self.name

    def __eq__(self, other):
        if not isinstance(other, Table):
            return False

        return self.name == other.name and tuple(self.columns) == tuple(other.columns)

    def __hash__(self):
        return hash((self.name, tuple(self.columns)))
