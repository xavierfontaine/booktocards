import copy
from typing import List

from jamdict import Jamdict, jmdict

from booktocards.annotations import Kanji
from booktocards.datacl import KanjiInfo, ParsedDictEntry
from booktocards.text import get_unique_kanjis

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
    parsed_dict_entry.kana_forms = [kana["text"] for kana in dictified_entry["kana"]]
    # kanji_forms
    parsed_dict_entry.kanji_forms = [kanj["text"] for kanj in dictified_entry["kanji"]]
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


def get_kanji_info(kanji: str) -> KanjiInfo:
    """Get inormation about a kanji from jamdict

    Args:
        kanji (str): kanji

    Returns:
        KanjiInfo
    """
    # Sanity checks
    if len(kanji) != 1:
        raise ValueError(f"`kanji` should be of length 1, but is {kanji=}")
    if len(get_unique_kanjis(kanji)) == 0:
        raise ValueError(f"`kanji` should be a kanji but is {kanji=}")
    # Get character entry
    jmd_entries = jam.lookup(
        query=kanji,
        strict_lookup=True,
    )
    char_entries = jmd_entries.chars
    assert len(char_entries) == 1, (
        "Code assumed that LookupResults.chars is always of length 1, but is"
        f" {char_entries}. Modify code appropriately."
    )
    char_entry = char_entries[0]
    del char_entries
    # Sanity
    assert len(char_entry.to_dict()["rm"]) == 1, (
        "Code assumed that len(LookupResults.chars[0].rm_groups) == 1 always, but"
        f" is {char_entry.to_dict()['rm']}. Modify code appropriately."
    )
    # Parse character entry
    readings_meanings = char_entry.to_dict()["rm"][0]
    meanings = [
        mean["value"] for mean in readings_meanings["meanings"] if mean["m_lang"] == ""
    ]
    onyomis = [
        read["value"]
        for read in readings_meanings["readings"]
        if read["type"] == "ja_on"
    ]
    kunyomis = [
        read["value"]
        for read in readings_meanings["readings"]
        if read["type"] == "ja_kun"
    ]
    nanoris = char_entry.to_dict()["nanoris"]
    freq = char_entry.to_dict()["freq"]
    jlpt = char_entry.to_dict()["jlpt"]
    # Put that into a KanjiInfo, and return it
    kanji_info = KanjiInfo(
        kanji=kanji,
        meanings=meanings,
        onyomis=onyomis,
        kunyomis=kunyomis,
        nanoris=nanoris,
        freq=freq,
        jlpt=jlpt,
    )
    return kanji_info
