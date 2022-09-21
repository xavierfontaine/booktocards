import copy
from jamdict import Jamdict, jmdict
from typing import List
from booktocards.datacl import ParsedDictEntry

from booktocards.annotations import Kanji

jam = Jamdict(memory_mode=True)


def get_definition(lemma: str, strict_lookup: bool = False) -> str:
    """Get definitions with kana and kanjis

    Args:
        lemma (str): lemma
        strict_lookup (bool): does not account for variants

    Returns:
        str
    """
    # Get jmdic entries
    jmd_entries: jmdict.JMDEntry = jam.lookup(
        query=lemma,
        strict_lookup=strict_lookup,
    ).entries
    # Extrac definitions
    def_list = [e.text(compact=True, no_id=False) for e in jmd_entries]
    definition = "\n".join(def_list)
    return definition


def has_frequent_reading(entry: jmdict.JMDEntry) -> bool:
    """Is one of the kanji/kana reading frequent for that entry?"""
    for forms in [entry.kanji_forms, entry.kana_forms]:
        for form in forms:
            if form.pri != []:
                return True
    return False


def drop_unfrequent_entries(
    entries: List[jmdict.JMDEntry],
) -> List[jmdict.JMDEntry]:
    """If one entry is frequent, drop the others, keep them all otherwise"""
    are_frequent = [has_frequent_reading(entry=entry) for entry in entries]
    if any(are_frequent):
        entries = [entry for entry, freq in zip(entries, are_frequent) if freq]
    return entries


def drop_unfrequent_readings(entry: jmdict.JMDEntry) -> jmdict.JMDEntry:
    """For each form (kanji, kana), drop unfrequent readings if one frequent
    exists"""
    entry = copy.deepcopy(entry)
    # Kanji
    if any([form.pri != [] for form in entry.kanji_forms]):
        filtered_forms = [form for form in entry.kanji_forms if form.pri != []]
        entry.kanji_forms = filtered_forms
    # Kana
    if any([form.pri != [] for form in entry.kana_forms]):
        filtered_forms = [form for form in entry.kana_forms if form.pri != []]
        entry.kana_forms = filtered_forms
    # Garbage
    return entry


def get_dict_entries(
    query: str,
    drop_unfreq_entries: bool,
    drop_unfreq_readings: bool,
    strict_lookup: bool = True,
) -> List[jmdict.JMDEntry]:
    """Return all entries corresponding to the query

    Args:
        query (str): query
        drop_unfreq_entries (bool): if 1+ returned entry is frequent, drop the
            others?
        drop_unfreq_readings (bool): in one entry, if 1+ reading is frequent,
            drop the others? (separately for kana and kanji reading)
        strict_lookup (bool): strict lookup of the query? (consider alternative
            readings etc.)

    Returns:
        List[jmdict.JMDEntry]
    """
    entries = jam.lookup(query=query, strict_lookup=strict_lookup).entries
    if drop_unfreq_entries:
        entries = drop_unfrequent_entries(entries=entries)
    if drop_unfreq_readings:
        entries = [drop_unfrequent_readings(entry=entry) for entry in entries]
    return entries


def parse_dict_entry(entry: jmdict.JMDEntry) -> ParsedDictEntry:
    """Parse a dict_entry from jamdict

    Args:
        entry (jmdict.JMDEntry): entry

    Returns:
        ParsedDictEntry
    """
    parsed_dict_entry = ParsedDictEntry()
    # Cast entry to dict
    dictified_entry = entry.to_dict()
    # entry_id
    parsed_dict_entry.entry_id = dictified_entry["idseq"]
    # kana_forms
    parsed_dict_entry.kana_forms = [
        kana["text"] for kana in dictified_entry["kana"]
    ]
    # kanji_forms
    parsed_dict_entry.kanji_forms = [
        kanj["text"] for kanj in dictified_entry["kanji"]
    ]
    # meanings
    parsed_dict_entry.meanings = [
        gloss["text"]
        for sense in dictified_entry["senses"]
        for gloss in sense["SenseGloss"]
    ]
    # is_frequent
    parsed_dict_entry.is_frequent = any(
        ["pri" in kana.keys() for kana in dictified_entry["kana"]]
    ) or any(["pri" in kanji.keys() for kanji in dictified_entry["kanji"]])
    return parsed_dict_entry
