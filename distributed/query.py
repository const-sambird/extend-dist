import re

from common.util import powerset

class Query:
    def __init__(self, query_text, coldict):
        self.text = query_text
        self.columns = []
        self.all_column_names = coldict

        self.columns = self.extract_columns()
    
    def extract_columns(self):
        REGEX = 'WHERE (.+?) (?:\\)|group by|order by)'
        columns = set()

        predicates = re.findall(REGEX, self.text, re.IGNORECASE)

        for predicate in predicates:
            for column in self.all_column_names:
                if column in predicate:
                    columns.add(column)

        return list(columns)
    
    def derived_candidate_indexes(self):
        if self.candidate_indexes is None:
            self.candidate_indexes = set(powerset(self.columns))
        
        return self.candidate_indexes
    
    def similarity(self, other) -> float:
        '''
        Computes the Jaccard similarity of the two queries
        by the sets of candidate indexes that they generate.
        '''
        this_candidates = self.derived_candidate_indexes()
        other_candidates = other.derived_candidate_indexes()

        return len(this_candidates & other_candidates) / len(this_candidates | other_candidates)
