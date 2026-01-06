import numpy as np
from typing import Iterable, Dict, Any, List
from collections import Counter
from numbers import Number

from booktocards.annotations import Token, Count


def ordered_counts(it: Iterable) -> Dict[Any, Count]:
    """Count and return counts by order of item appearance

    Args:
        it (Iterable)

    Returns:
        Dict[Any, Count]
    """
    # Count
    counts = Counter(it)
    # Sort by appearance
    unique = ordered_unique(it=it)
    counts_dict = {k: counts[k] for k in unique}
    return counts_dict


def ordered_unique(it: Iterable) -> list:
    """Same as set, but ordered by appearance order

    Args:
        it (Iterable)

    Returns:
        list
    """
    return list(dict.fromkeys(it))


def argsort(l: List[int | float]) -> List[int]:
    """Return decreasing sorting as positions"""
    index = list(np.argsort(a=l))
    index.reverse()
    return index
