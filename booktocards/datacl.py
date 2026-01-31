import copy
import re
from dataclasses import dataclass, field
from typing import List, Optional

from jamdict import jmdict

from booktocards.annotations import (
    Definition,
    Reading,
    Sentence,
    SentenceId,
)


@dataclass
class ParsedDictEntry:
    """Parsed jamdict.jmdict.JMDEntry

    Contains the information from a JMDEntry that are most relevant to the
    current project.
    """

    entry_id: Optional[int] = None
    kana_forms: Optional[List[str]] = None
    is_frequent: Optional[bool] = None
    kanji_forms: Optional[List[str]] = None
    meanings: Optional[List[str]] = None


@dataclass
class TokenInfo:
    """Information about a token"""

    lemma: str
    pos: Optional[str] = None
    count: Optional[int] = None
    dict_entries: Optional[List[jmdict.JMDEntry]] = None
    parsed_dict_entries: Optional[List[ParsedDictEntry]] = None
    sanseido_dict_entries: Optional[dict[Reading, list[Definition]]] = None
    source_sent_ids: Optional[List[SentenceId]] = None
    source_ex_str: Optional[List[Sentence]] = None
    source_ex_str_transl: Optional[List[Sentence]] = None
    tatoeba_ex_str: Optional[List[Sentence]] = None
    tatoeba_ex_str_transl: Optional[List[Sentence]] = None
    source_name_str: Optional[str] = None


@dataclass
class VocabCard:
    """Fields of a vocabulary flashcard

    Each attribute is a str, an int or a None. Hence, a VocabCard can be
    readily turned into the content of a flashcard.
    """

    entry_id: int
    lemma: str
    count: Optional[int] = None
    kana_forms_str: Optional[str] = None
    kanji_forms_str: Optional[str] = None
    is_frequent: Optional[bool] = None
    meanings_str: Optional[str] = None
    sanseido_def_str: Optional[str] = None
    examples_str: Optional[str] = None
    source_name_str: Optional[str] = None


def token_info_to_voc_cards(
    token_info: TokenInfo,
    ex_linebreak_repl: Optional[str] = None,
    source_name: Optional[str] = None,
) -> List[VocabCard]:
    """Transform a TokenInfo into a VocabCard

    Args:
        token_info (TokenInfo)

    Returns:
        List[VocabCard]: one dict per entry in token_info
    """
    cards = list()
    token_info = copy.deepcopy(token_info)
    # Assert the expected fields of token_info are present
    assert token_info.parsed_dict_entries is not None
    # Create each card
    for entry in token_info.parsed_dict_entries:
        # Assert the expected fields of each parsed_dict_entry are present
        assert entry.entry_id is not None
        assert entry.kana_forms is not None
        assert entry.kanji_forms is not None
        assert entry.meanings is not None
        # Make card
        card = VocabCard(entry_id=entry.entry_id, lemma=token_info.lemma)
        card.count = token_info.count
        card.kana_forms_str = ", ".join(entry.kana_forms)
        card.kanji_forms_str = ", ".join(entry.kanji_forms)
        card.is_frequent = entry.is_frequent
        card.meanings_str = (
            "# " + "\n# ".join(entry.meanings)
            if len(entry.meanings) > 1
            else entry.meanings[0]
        )
        # Add source examples (w translation when available)
        card.examples_str = ""
        if (
            token_info.source_ex_str is not None and token_info.source_ex_str != []
        ) and (
            token_info.source_ex_str_transl is not None
            and token_info.source_ex_str_transl != []
        ):
            source_ex_w_transl = token_info.source_ex_str
            if ex_linebreak_repl is not None:
                source_ex_w_transl = [
                    re.sub("[\n\r]+", " // ", ex) for ex in source_ex_w_transl
                ]
            if source_name is not None:
                source_ex_w_transl = [
                    f"[{source_name}] " + ex for ex in source_ex_w_transl
                ]
            if token_info.source_ex_str_transl not in [None, []]:
                transl_ex = token_info.source_ex_str_transl
                if ex_linebreak_repl is not None:
                    transl_ex = [re.sub("[\n\r]+", " // ", ex) for ex in transl_ex]
                for i, transl in enumerate(transl_ex):
                    source_ex_w_transl[i] += f" ({transl})"
            card.examples_str += "# " + "\n# ".join(source_ex_w_transl)
        # Add tatoeba examples
        if (
            token_info.tatoeba_ex_str is not None and token_info.tatoeba_ex_str != []
        ) and (
            token_info.tatoeba_ex_str_transl is not None
            and token_info.tatoeba_ex_str_transl != []
        ):
            if card.examples_str != "":
                card.examples_str += "\n"
            tatoeba_ext_str = token_info.tatoeba_ex_str
            tatoeba_ext_str_transl = token_info.tatoeba_ex_str_transl
            if ex_linebreak_repl is not None:
                tatoeba_ext_str = [
                    re.sub("[\n\r]+", " // ", ex) for ex in tatoeba_ext_str
                ]
                tatoeba_ext_str_transl = [
                    re.sub("[\n\r]+", " // ", ex) for ex in tatoeba_ext_str_transl
                ]
            tatoeba_ex_w_transl = [
                f"[tatoeba] {jpn} ({eng})"
                for jpn, eng in zip(tatoeba_ext_str, tatoeba_ext_str_transl)
            ]
            card.examples_str += "# " + "\n# ".join(tatoeba_ex_w_transl)
        # Add jj definition
        if (
            token_info.sanseido_dict_entries is not None
            and token_info.sanseido_dict_entries != []
        ):
            per_reading_def = [
                f"Reading {i}) {reading}\n[def] " + "[def] ".join(defs)
                for i, (reading, defs) in enumerate(
                    token_info.sanseido_dict_entries.items()
                )
            ]
            card.sanseido_def_str = "\n".join(per_reading_def)
        else:
            card.sanseido_def_str = ""
        # Add source name
        card.source_name_str = token_info.source_name_str
        # Append current card ot set of output cards
        cards.append(card)
    return cards


@dataclass
class KanjiInfo:
    """Information about a kanji"""

    kanji: str
    meanings: list[str]
    onyomis: list[str] = field(default_factory=list)
    kunyomis: list[str] = field(default_factory=list)
    nanoris: list[str] = field(default_factory=list)
    freq: Optional[int] = None
    jlpt: Optional[int] = None
    seen_in_tokens: list[str] = field(default_factory=list)
    source_name: Optional[str] = None


@dataclass
class KanjiCard:
    """Fields of a kanji flashcard

    Each attribute is a str, an int or a None. Hence, a KanjiCard can be
    readily turned into the content of a flashcard.
    """

    kanji: str
    meanings_str: str
    onyomis_str: Optional[str] = None
    kunyomis_str: Optional[str] = None
    nanoris_str: Optional[str] = None
    freq: Optional[int] = None
    jlpt: Optional[int] = None
    seen_in_tokens_str: Optional[str] = None
    source_name: Optional[str] = None


def kanji_info_to_kanji_card(kanji_info: KanjiInfo) -> KanjiCard:
    """Transform a KanjiInfo into a KanjiCard

    Args:
        kanji_info (KanjiInfo)

    Returns:
        KanjiCard
    """
    # Get card items
    kanji = kanji_info.kanji
    meanings_str = "# " + "\n# ".join(kanji_info.meanings)
    if kanji_info.onyomis is not None:
        onyomis_str = ", ".join(kanji_info.onyomis)
    if kanji_info.kunyomis is not None:
        kunyomis_str = ", ".join(kanji_info.kunyomis)
    if kanji_info.nanoris is not None:
        nanoris_str = ", ".join(kanji_info.nanoris)
    freq = kanji_info.freq
    jlpt = kanji_info.jlpt
    if kanji_info.seen_in_tokens is not None:
        seen_in_tokens_str = ", ".join(kanji_info.seen_in_tokens)
    jlpt = kanji_info.jlpt
    source_name = kanji_info.source_name
    # Make card
    kanji_card = KanjiCard(
        kanji=kanji,
        meanings_str=meanings_str,
        onyomis_str=onyomis_str,
        kunyomis_str=kunyomis_str,
        nanoris_str=nanoris_str,
        freq=freq,
        jlpt=jlpt,
        seen_in_tokens_str=seen_in_tokens_str,
        source_name=source_name,
    )
    return kanji_card
