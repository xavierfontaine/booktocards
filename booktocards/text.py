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


def is_only_ascii_alphanum(text: str) -> bool:
    """Is only ascii alphanum? ([a-zA-Z0-9])"""
    return bool(re.match(pattern=r"^[a-zA-Z0-9]+$", string=text))
