"""
Utils for dealing with text
"""
import re


def get_unique_kanjis(doc: str) -> list[str]:
    """Return list of unique kanjis from doc"""
    kanjis = re.findall(
        pattern=r"[\u4E00-\u9FAF]",
        string=doc,
    )
    unique_kanjis = list(set(kanjis))
    return unique_kanjis
