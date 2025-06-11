from itertools import chain, combinations

def powerset(iterable, max_len = None):
    # https://docs.python.org/2/library/itertools.html#recipes
    s = list(iterable)
    if max_len is not None:
        return chain.from_iterable(combinations(s, r) for r in range(min(len(s), max_len)+1))
    else:
        return chain.from_iterable(combinations(s, r) for r in range(len(s)+1))
    
def b_to_mb(b):
    return b / 1000 / 1000


def mb_to_b(mb):
    return mb * 1000 * 1000